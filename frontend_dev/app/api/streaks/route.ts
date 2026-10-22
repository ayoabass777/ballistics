import { NextRequest, NextResponse } from "next/server"
import { mockStreaks, type StreakData, type StreakScope, type StreakType } from "@/lib/mock-streaks"
import { enrichStreakBranding } from "@/lib/streak-branding"
import { frontendLogoAssetPath, rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheHeader, redisCacheKey, redisCacheTtl, type RedisCacheMeta } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const PUBLIC_API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "")
const HOME_LIMIT = 20
const EXPLORE_RESTORE_LIMIT_MAX = 1000
const HOME_STREAKS_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_HOME_STREAKS_CACHE_TTL_SECONDS", 60)
const EXPLORE_STREAKS_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_EXPLORE_STREAKS_CACHE_TTL_SECONDS", 120)

type BackendStreak = {
  streak_id: string | number
  team_id: number | string
  event_name?: string
  metric_key?: string
  headline?: string
  streak_type?: string
  current_streak_length: number
  team_name: string
  league_name: string
  next_kickoff_utc?: string | null
  next_opponent_name?: string | null
  next_is_home?: boolean | null
  next_fixture_id?: string | number | null
  context_scope?: string | null
  team_logo_url?: string | null
  league_logo_url?: string | null
  country_name?: string | null
  country_logo_url?: string | null
  team_logo_file_id?: string | null
  league_logo_file_id?: string | null
  country_logo_file_id?: string | null
  recent_form?: unknown
  // Bayesian model outputs — sent by FastAPI when available
  confidence_score?: number | null
  team_tier?: number | null
  opponent_tier?: number | null
}

type ScopeMode = "home" | "explore"
type ExplorePagination = {
  cursor?: string | null
  next_cursor?: string | null
  nextCursor?: string | null
  limit?: number
  total?: number
  hasMore?: boolean
  sort?: "default" | "match_date_asc"
}

type BackendLeagueOption = {
  id?: string
  league_name?: string
  country_name?: string | null
  label?: string
  count?: number
  goal_streaks_count?: number
  result_streaks_count?: number
}

type BackendExploreFilters = {
  leagues?: string[]
  types?: string[]
  league_options?: BackendLeagueOption[]
  type_counts?: {
    all?: number
    goal_streaks?: number
    result_streaks?: number
  }
}

type ExploreLeagueOption = {
  id: string
  league_name: string
  country_name?: string | null
  league_logo_url?: string | null
  country_logo_url?: string | null
  label: string
  count?: number
  goal_streaks_count?: number
  result_streaks_count?: number
}

type ExploreTypeCounts = {
  all: number
  goal_streaks: number
  result_streaks: number
}

type ExploreFilters = {
  leagues: string[]
  types: string[]
  league_options: ExploreLeagueOption[]
  type_counts: ExploreTypeCounts
}

type BackendRowsResult = {
  rows: BackendStreak[]
  pagination?: ExplorePagination
  filters?: BackendExploreFilters
  cache?: RedisCacheMeta
}

type ExploreSortMode = "default" | "match_date_asc"

function parseScope(request: NextRequest): ScopeMode {
  const scope = request.nextUrl.searchParams.get("scope")
  return scope === "explore" ? "explore" : "home"
}

function parseExploreCursor(request: NextRequest): string | undefined {
  const value = request.nextUrl.searchParams.get("cursor")
  return value && value.trim() ? value.trim() : undefined
}

function parseExploreLimit(request: NextRequest): number {
  const raw = request.nextUrl.searchParams.get("limit")
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) return 50
  return Math.min(Math.max(Math.trunc(parsed), 1), 50)
}

function parseExploreRestoreLimit(request: NextRequest): number | undefined {
  const raw = request.nextUrl.searchParams.get("restoreLimit")
  const parsed = Number(raw)
  if (!Number.isFinite(parsed)) return undefined
  return Math.min(Math.max(Math.trunc(parsed), 1), EXPLORE_RESTORE_LIMIT_MAX)
}

function parseExploreSort(request: NextRequest): ExploreSortMode {
  return request.nextUrl.searchParams.get("sort") === "match_date_asc" ? "match_date_asc" : "default"
}

function sortExploreRows(rows: StreakData[], sort: ExploreSortMode): StreakData[] {
  if (sort !== "match_date_asc") return rows

  return [...rows].sort((a, b) => {
    const aTime = a.next_match_date ? Date.parse(a.next_match_date) : Number.POSITIVE_INFINITY
    const bTime = b.next_match_date ? Date.parse(b.next_match_date) : Number.POSITIVE_INFINITY
    const normalizedATime = Number.isFinite(aTime) ? aTime : Number.POSITIVE_INFINITY
    const normalizedBTime = Number.isFinite(bTime) ? bTime : Number.POSITIVE_INFINITY
    if (normalizedATime !== normalizedBTime) return normalizedATime - normalizedBTime
    if (a.current_streak_length !== b.current_streak_length) return b.current_streak_length - a.current_streak_length
    return a.team_name.localeCompare(b.team_name, undefined, { sensitivity: "base" })
  })
}

function normalizeStreakType(rawType: string | undefined): StreakType | null {
  if (rawType === "goal" || rawType === "goal_streaks") return "goal_streaks"
  if (rawType === "result" || rawType === "result_streaks") return "result_streaks"
  return null
}

function normalizeScope(rawScope: string | null | undefined): StreakScope {
  if (rawScope === "home" || rawScope === "away") return rawScope
  return "overall"
}

function deriveMetricKeyFromEventName(eventName: string | undefined): string {
  const normalized = (eventName || "event")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_")
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/_+/g, "_")

  const aliases: Record<string, string> = {
    score_1_goal: "score_1goal",
    score_1_goals: "score_1goal",
    score1goal: "score_1goal",
    score1goals: "score_1goal",
    score_2_goal: "score_2goals",
    score_2_goals: "score_2goals",
    score2goal: "score_2goals",
    score2goals: "score_2goals",
    concede_1_goal: "concede_1",
    concede_1_goals: "concede_1",
    conceded_1: "concede_1",
    conceded_1_goal: "concede_1",
    conceded_1_goals: "concede_1",
    concede_2_goal: "concede_2",
    concede_2_goals: "concede_2",
    conceded_2: "concede_2",
    conceded_2_goal: "concede_2",
    conceded_2_goals: "concede_2",
    btts: "BTTS",
    over_1_5_goals: "Over_1_5",
    over_2_5_goals: "Over_2_5",
    over_3_5_goals: "Over_3_5",
    over_4_5_goals: "Over_4_5",
    under_1_5_goals: "Under_1_5",
    under_2_5_goals: "Under_2_5",
    under_3_5_goals: "Under_3_5",
    under_4_5_goals: "Under_4_5",
  }
  if (aliases[normalized]) return aliases[normalized]

  const specialCase: Record<string, string> = {
    over_1_5: "Over_1_5",
    over_2_5: "Over_2_5",
    over_3_5: "Over_3_5",
    over_4_5: "Over_4_5",
    under_1_5: "Under_1_5",
    under_2_5: "Under_2_5",
    under_3_5: "Under_3_5",
    under_4_5: "Under_4_5",
  }
  return specialCase[normalized] || normalized || "event"
}

function canonicalMetricKey(metricKey: string | undefined, eventName: string | undefined): string {
  const provided = metricKey?.trim()
  if (provided) return provided
  return deriveMetricKeyFromEventName(eventName)
}

function fallbackHeadline(streakLength: number, metricKey: string): string {
  const source = (metricKey || "event").replace(/_/g, " ").trim()
  return `${streakLength} ${source}`.trim()
}

function stablePositiveInt(seed: string): number {
  let hash = 2166136261
  for (let i = 0; i < seed.length; i += 1) {
    hash ^= seed.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  const value = hash >>> 0
  return value === 0 ? 1 : value
}

function parseStreakId(row: BackendStreak, index: number): number {
  const streakId = row.streak_id
  const numeric = Number(streakId)
  if (Number.isFinite(numeric)) return Math.trunc(numeric)

  const seed = [
    String(streakId ?? "").trim(),
    String(row.team_id ?? ""),
    row.metric_key?.trim() || "",
    row.event_name?.trim() || "",
    row.streak_type?.trim() || "",
    row.context_scope?.trim() || "",
    String(row.current_streak_length ?? ""),
  ]
    .filter(Boolean)
    .join("|")

  return stablePositiveInt(seed || `streak-row-${index + 1}`)
}

function normalizeRecentForm(value: unknown): StreakData["recent_form"] | undefined {
  if (!Array.isArray(value)) return undefined

  const normalized = value.filter((item): item is "W" | "D" | "L" => item === "W" || item === "D" || item === "L")
  return normalized.length > 0 ? normalized : undefined
}

function backendLogoUrlFromFileId(fileId: string | undefined | null): string | undefined {
  return frontendLogoAssetPath(fileId)
}

function resolvePublicApiBaseUrl(request: NextRequest): string | undefined {
  const fallback = PUBLIC_API_BASE_URL || BACKEND_API_BASE_URL
  const hostHeader =
    request.headers.get("x-forwarded-host") || request.headers.get("host") || request.nextUrl.host || ""
  const protocolHeader =
    request.headers.get("x-forwarded-proto") || request.nextUrl.protocol.replace(":", "") || "http"

  const hostname = hostHeader.split(":")[0]?.trim()
  if (!hostname) return fallback

  const derived = `${protocolHeader}://${hostname}:8000`
  if (!PUBLIC_API_BASE_URL) return derived

  try {
    const configured = new URL(PUBLIC_API_BASE_URL)
    const configuredLocal = configured.hostname === "localhost" || configured.hostname === "127.0.0.1"
    const requestLocal = hostname === "localhost" || hostname === "127.0.0.1"
    if (configuredLocal && !requestLocal) return derived
  } catch {
    return derived
  }

  return PUBLIC_API_BASE_URL
}

function backendLogoUrlFromFileIdWithPublicBase(
  fileId: string | undefined | null,
  _publicApiBaseUrl: string | undefined,
): string | undefined {
  return frontendLogoAssetPath(fileId)
}

function rewriteBackendAssetUrl(url: string | undefined | null, publicApiBaseUrl?: string): string | undefined {
  return rewriteAssetUrlToFrontend(url)
}

function toSlug(value: string | null | undefined): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
  return normalized || "item"
}

function toLeagueOptionId(leagueName: string, countryName?: string | null): string {
  return `${toSlug(countryName || "unknown")}-${toSlug(leagueName)}`
}

function normalizeExploreFilters(raw: BackendExploreFilters | undefined, fallbackRows: StreakData[]): ExploreFilters {
  const fallbackLogosByLeague = new Map<string, { league_logo_url?: string; country_logo_url?: string }>()
  for (const row of fallbackRows) {
    const leagueName = String(row.league_name || "").trim()
    if (!leagueName) continue
    const key = `${leagueName}::${String(row.country_name || "").trim()}`
    const current = fallbackLogosByLeague.get(key) || {}
    if (!current.league_logo_url && row.league_logo_url) current.league_logo_url = row.league_logo_url
    if (!current.country_logo_url && row.country_logo_url) current.country_logo_url = row.country_logo_url
    fallbackLogosByLeague.set(key, current)
  }

  const leagues = Array.isArray(raw?.leagues)
    ? raw.leagues.map((item) => String(item || "").trim()).filter(Boolean)
    : []
  const types = Array.isArray(raw?.types)
    ? raw.types.map((item) => String(item || "").trim()).filter(Boolean)
    : []

  const usedIds = new Set<string>()
  const normalizedOptions: ExploreLeagueOption[] = []
  if (Array.isArray(raw?.league_options)) {
    for (const item of raw.league_options) {
      const leagueName = String(item?.league_name || "").trim()
      if (!leagueName) continue
      const countryName = String(item?.country_name || "").trim() || undefined
      const label = String(item?.label || "").trim() || (countryName ? `${leagueName} (${countryName})` : leagueName)
      const baseId = String(item?.id || "").trim() || toLeagueOptionId(leagueName, countryName)
      let id = baseId
      let suffix = 2
      while (usedIds.has(id)) {
        id = `${baseId}-${suffix}`
        suffix += 1
      }
      usedIds.add(id)
      const fallbackLogos = fallbackLogosByLeague.get(`${leagueName}::${countryName || ""}`)
      normalizedOptions.push({
        id,
        league_name: leagueName,
        country_name: countryName,
        league_logo_url:
          String((item as any)?.league_logo_url || (item as any)?.logo_url || "").trim() ||
          fallbackLogos?.league_logo_url ||
          null,
        country_logo_url:
          String((item as any)?.country_logo_url || "").trim() || fallbackLogos?.country_logo_url || null,
        label,
        count: typeof item?.count === "number" ? item.count : undefined,
        goal_streaks_count: typeof item?.goal_streaks_count === "number" ? item.goal_streaks_count : undefined,
        result_streaks_count: typeof item?.result_streaks_count === "number" ? item.result_streaks_count : undefined,
      })
    }
  }

  if (normalizedOptions.length === 0) {
    const byKey = new Map<string, ExploreLeagueOption>()
    for (const row of fallbackRows) {
      const leagueName = String(row.league_name || "").trim()
      if (!leagueName) continue
      const countryName = String(row.country_name || "").trim() || undefined
      const key = `${leagueName}::${countryName || ""}`
      const current = byKey.get(key)
      if (current) {
        current.count = (current.count || 0) + 1
        if (row.streak_type === "goal_streaks") {
          current.goal_streaks_count = (current.goal_streaks_count || 0) + 1
        }
        if (row.streak_type === "result_streaks") {
          current.result_streaks_count = (current.result_streaks_count || 0) + 1
        }
        continue
      }
      byKey.set(key, {
        id: toLeagueOptionId(leagueName, countryName),
        league_name: leagueName,
        country_name: countryName,
        league_logo_url: row.league_logo_url || null,
        country_logo_url: row.country_logo_url || null,
        label: countryName ? `${leagueName} (${countryName})` : leagueName,
        count: 1,
        goal_streaks_count: row.streak_type === "goal_streaks" ? 1 : 0,
        result_streaks_count: row.streak_type === "result_streaks" ? 1 : 0,
      })
    }
    normalizedOptions.push(...Array.from(byKey.values()))
  }

  normalizedOptions.sort((a, b) => {
    const countryCmp = String(a.country_name || "").localeCompare(String(b.country_name || ""), undefined, {
      sensitivity: "base",
    })
    if (countryCmp !== 0) return countryCmp
    return a.league_name.localeCompare(b.league_name, undefined, { sensitivity: "base" })
  })

  const fallbackTypeCounts = fallbackRows.reduce(
    (acc, row) => {
      acc.all += 1
      if (row.streak_type === "goal_streaks") acc.goal_streaks += 1
      if (row.streak_type === "result_streaks") acc.result_streaks += 1
      return acc
    },
    { all: 0, goal_streaks: 0, result_streaks: 0 } as ExploreTypeCounts,
  )
  const rawTypeCounts = raw?.type_counts
  const typeCounts: ExploreTypeCounts = {
    all: typeof rawTypeCounts?.all === "number" ? rawTypeCounts.all : fallbackTypeCounts.all,
    goal_streaks:
      typeof rawTypeCounts?.goal_streaks === "number" ? rawTypeCounts.goal_streaks : fallbackTypeCounts.goal_streaks,
    result_streaks:
      typeof rawTypeCounts?.result_streaks === "number"
        ? rawTypeCounts.result_streaks
        : fallbackTypeCounts.result_streaks,
  }

  return {
    leagues: leagues.length > 0 ? leagues : Array.from(new Set(normalizedOptions.map((opt) => opt.league_name))).sort(),
    types: types.length > 0 ? types : Array.from(new Set(fallbackRows.map((row) => row.streak_type))).sort(),
    league_options: normalizedOptions,
    type_counts: typeCounts,
  }
}

function adaptBackendStreak(row: BackendStreak, index: number): StreakData | null {
  const streakType = normalizeStreakType(row.streak_type)
  if (!streakType) return null

  const metricKey = canonicalMetricKey(row.metric_key, row.event_name)
  const headline = row.headline?.trim() || fallbackHeadline(row.current_streak_length, metricKey)
  const fixtureId =
    row.next_fixture_id !== null && row.next_fixture_id !== undefined
      ? String(row.next_fixture_id)
      : undefined
  return adaptBackendStreakWithPublicBase(row, index, PUBLIC_API_BASE_URL || undefined)
}

function adaptBackendStreakWithPublicBase(
  row: BackendStreak,
  index: number,
  publicApiBaseUrl: string | undefined,
): StreakData | null {
  const streakType = normalizeStreakType(row.streak_type)
  if (!streakType) return null

  const metricKey = canonicalMetricKey(row.metric_key, row.event_name)
  const headline = row.headline?.trim() || fallbackHeadline(row.current_streak_length, metricKey)
  const fixtureId =
    row.next_fixture_id !== null && row.next_fixture_id !== undefined
      ? String(row.next_fixture_id)
      : undefined
  const teamLogoUrl =
    rewriteBackendAssetUrl(row.team_logo_url, publicApiBaseUrl) ||
    backendLogoUrlFromFileIdWithPublicBase(row.team_logo_file_id, publicApiBaseUrl)

  const enriched = enrichStreakBranding({
    streak_id: parseStreakId(row, index),
    metric_key: metricKey,
    headline,
    streak_type: streakType,
    current_streak_length: row.current_streak_length,
    team_name: row.team_name,
    league_name: row.league_name,
    next_match_date: row.next_kickoff_utc || undefined,
    next_opponent: row.next_opponent_name || undefined,
    next_is_home: typeof row.next_is_home === "boolean" ? row.next_is_home : undefined,
    fixture_id: fixtureId,
    scope: normalizeScope(row.context_scope),
    team_logo_url: teamLogoUrl,
    league_logo_url:
      rewriteBackendAssetUrl(row.league_logo_url, publicApiBaseUrl) ||
      backendLogoUrlFromFileIdWithPublicBase(row.league_logo_file_id, publicApiBaseUrl),
    country_name: row.country_name || undefined,
    country_logo_url:
      rewriteBackendAssetUrl(row.country_logo_url, publicApiBaseUrl) ||
      backendLogoUrlFromFileIdWithPublicBase(row.country_logo_file_id, publicApiBaseUrl),
    recent_form: normalizeRecentForm(row.recent_form),
    confidence_score: typeof row.confidence_score === "number" ? row.confidence_score : undefined,
    team_tier: typeof row.team_tier === "number" ? row.team_tier : undefined,
    opponent_tier: typeof row.opponent_tier === "number" ? row.opponent_tier : undefined,
  })

  return enriched
}

async function fetchBackendRows(
  scope: ScopeMode,
  cursor?: string,
  limit?: number,
  sort: ExploreSortMode = "default",
  restoreLimit?: number,
): Promise<BackendRowsResult> {
  if (!BACKEND_API_BASE_URL) {
    throw new Error("BACKEND_API_BASE_URL is not configured")
  }

  const endpoint =
    scope === "explore"
      ? `/explore/streaks?${new URLSearchParams({
          ...(cursor ? { cursor } : {}),
          ...(limit ? { limit: String(limit) } : {}),
          ...(!cursor && restoreLimit ? { restoreLimit: String(restoreLimit) } : {}),
          ...(sort !== "default" ? { sort } : {}),
        }).toString()}`
      : "/home/streaks"
  const ttlSeconds = scope === "explore" ? EXPLORE_STREAKS_CACHE_TTL_SECONDS : HOME_STREAKS_CACHE_TTL_SECONDS
  const cacheKey = redisCacheKey("streaks", endpoint)
  const cached = await getOrSetRedisJson<unknown>(cacheKey, ttlSeconds, async () => {
    const response = await fetch(`${BACKEND_API_BASE_URL}${endpoint}`, {
      headers: { Accept: "application/json" },
      cache: "no-store",
    })
    if (!response.ok) {
      const text = await response.text()
      throw new Error(`Backend request failed (${response.status}): ${text}`)
    }

    return response.json()
  })

  const payload = cached.value as any
  if (scope === "explore") {
    if (Array.isArray(payload)) return { rows: payload, cache: cached.meta }
    if (Array.isArray(payload?.data?.streaks)) {
      return {
        rows: payload.data.streaks,
        pagination: payload.pagination || undefined,
        filters: payload.data.filters || payload.filters || undefined,
        cache: cached.meta,
      }
    }
    return { rows: [], cache: cached.meta }
  }

  if (Array.isArray(payload?.data?.streaks)) {
    return { rows: payload.data.streaks, cache: cached.meta }
  }
  return { rows: [], cache: cached.meta }
}

export async function GET(request: NextRequest) {
  const scope = parseScope(request)
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"
  const exploreCursor = parseExploreCursor(request)
  const exploreLimit = parseExploreLimit(request)
  const exploreRestoreLimit = parseExploreRestoreLimit(request)
  const effectiveExploreLimit = exploreRestoreLimit ?? exploreLimit
  const exploreSort = parseExploreSort(request)
  const publicApiBaseUrl = resolvePublicApiBaseUrl(request)

  if (useMockData) {
    if (scope === "explore") {
      const sortedMockStreaks = sortExploreRows(mockStreaks, exploreSort)
      const start = exploreCursor ? Number(exploreCursor) || 0 : 0
      const page = sortedMockStreaks.slice(start, start + effectiveExploreLimit)
      const nextIndex = start + page.length
      const hasMore = nextIndex < sortedMockStreaks.length
      return NextResponse.json({
        data: page,
        pagination: {
          cursor: exploreCursor || String(start),
          next_cursor: hasMore ? String(nextIndex) : null,
          limit: effectiveExploreLimit,
          total: sortedMockStreaks.length,
          hasMore,
          sort: exploreSort,
        },
        filters: normalizeExploreFilters(undefined, mockStreaks),
      })
    }
    const fallback = scope === "home" ? mockStreaks.slice(0, HOME_LIMIT) : mockStreaks
    return NextResponse.json(fallback)
  }

  try {
    const backend = await fetchBackendRows(
      scope,
      exploreCursor,
      scope === "explore" ? exploreLimit : undefined,
      exploreSort,
      scope === "explore" ? exploreRestoreLimit : undefined,
    )
    const adapted = backend.rows
      .map((row, index) => adaptBackendStreakWithPublicBase(row, index, publicApiBaseUrl))
      .filter((row): row is StreakData => row !== null)
    if (scope === "explore") {
      const pagination = backend.pagination || {}
      return NextResponse.json(
        {
          data: adapted,
          pagination: {
            cursor: pagination.cursor ?? exploreCursor ?? "0",
            next_cursor: pagination.next_cursor ?? pagination.nextCursor ?? null,
            limit: pagination.limit ?? effectiveExploreLimit,
            total: pagination.total ?? adapted.length,
            hasMore: Boolean(pagination.hasMore),
            sort: pagination.sort ?? exploreSort,
          },
          filters: normalizeExploreFilters(backend.filters, adapted),
        },
        { headers: { "x-cache": backend.cache ? redisCacheHeader(backend.cache) : "unknown" } },
      )
    }
    const result = adapted.slice(0, HOME_LIMIT)
    return NextResponse.json(result, {
      headers: { "x-cache": backend.cache ? redisCacheHeader(backend.cache) : "unknown" },
    })
  } catch (error) {
    console.error("Error in streaks API adapter:", error)
    if (scope === "explore") {
      const sortedMockStreaks = sortExploreRows(mockStreaks, exploreSort)
      const start = exploreCursor ? Number(exploreCursor) || 0 : 0
      const page = sortedMockStreaks.slice(start, start + effectiveExploreLimit)
      const nextIndex = start + page.length
      const hasMore = nextIndex < sortedMockStreaks.length
      return NextResponse.json(
        {
          data: page,
          pagination: {
            cursor: exploreCursor || String(start),
            next_cursor: hasMore ? String(nextIndex) : null,
            limit: effectiveExploreLimit,
            total: sortedMockStreaks.length,
            hasMore,
            sort: exploreSort,
          },
          filters: normalizeExploreFilters(undefined, mockStreaks),
        },
        { headers: { "x-data-source": "mock-fallback" } },
      )
    }
    const fallback = scope === "home" ? mockStreaks.slice(0, HOME_LIMIT) : mockStreaks
    return NextResponse.json(fallback, { headers: { "x-data-source": "mock-fallback" } })
  }
}
