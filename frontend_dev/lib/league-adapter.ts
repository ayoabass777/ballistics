import type { LeaguePageViewModel, LeagueTableRow, LeagueContextFilter, LeagueTopStreak } from "@/types/league-page"
import { rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheKey, redisCacheTtl, type RedisCacheMeta } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = (process.env.BACKEND_API_BASE_URL || process.env.NEXT_PUBLIC_API_BASE_URL)?.replace(/\/$/, "")
const LEAGUE_PAGE_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_LEAGUE_PAGE_CACHE_TTL_SECONDS", 300)

export type LeagueAdapterSuccess = {
  ok: true
  source: "backend"
  data: LeaguePageViewModel
  cache?: RedisCacheMeta
}

export type LeagueAdapterFailure = {
  ok: false
  kind: "not_found" | "error"
  status: number
  code: string
  error: string
  details?: string
}

export type LeagueAdapterResult = LeagueAdapterSuccess | LeagueAdapterFailure

type BackendLeaguePageResponse = {
  data?: {
    league?: {
      league_season_id?: string
      league_slug?: string
      league_name?: string
      season_label?: string
      country_name?: string | null
      league_logo_url?: string | null
      country_logo_url?: string | null
    }
    standings?: {
      overall?: Array<{
        position?: number | null
        team_id: string
        team_name: string
        team_slug?: string
        team_logo_url?: string | null
        played?: number | null
        wins?: number | null
        draws?: number | null
        losses?: number | null
        goals_for?: number | null
        goals_against?: number | null
        goal_difference?: number | null
        points?: number | null
        form?: string[]
      }>
    }
    top_streaks?: Array<{
      streak_id?: string | number
      team_id?: string | number
      event_name?: string
      metric_key?: string
      headline?: string
      streak_type?: string
      current_streak_length?: number | null
      team_name?: string
      league_name?: string
      country_name?: string | null
      next_kickoff_utc?: string | null
      next_opponent_name?: string | null
      next_fixture_id?: string | number | null
      next_is_home?: boolean | null
      context_scope?: string | null
      team_logo_url?: string | null
      league_logo_url?: string | null
      country_logo_url?: string | null
    }>
    available_contexts?: string[]
  }
}

type BackendStandingRow = {
  position?: number | null
  team_id: string
  team_name: string
  team_slug?: string
  team_logo_url?: string | null
  played?: number | null
  wins?: number | null
  draws?: number | null
  losses?: number | null
  goals_for?: number | null
  goals_against?: number | null
  goal_difference?: number | null
  points?: number | null
  form?: string[]
}

type BackendTopStreakRow = {
  streak_id?: string | number
  team_id?: string | number
  event_name?: string
  metric_key?: string
  headline?: string
  streak_type?: string
  current_streak_length?: number | null
  team_name?: string
  league_name?: string
  country_name?: string | null
  next_kickoff_utc?: string | null
  next_opponent_name?: string | null
  next_fixture_id?: string | number | null
  next_is_home?: boolean | null
  context_scope?: string | null
  team_logo_url?: string | null
  league_logo_url?: string | null
  country_logo_url?: string | null
}

function rewriteBackendAssetUrl(url: string | undefined | null): string | undefined {
  return rewriteAssetUrlToFrontend(url)
}

function toTableRow(row: BackendStandingRow): LeagueTableRow {
  const form = Array.isArray(row.form) ? row.form.filter((v: string): v is "W" | "D" | "L" => v === "W" || v === "D" || v === "L") : []
  return {
    position: row.position ?? null,
    teamId: String(row.team_id),
    teamName: String(row.team_name || ""),
    teamSlug: row.team_slug,
    teamLogo: rewriteBackendAssetUrl(row.team_logo_url || undefined),
    points: row.points ?? null,
    form,
    played: row.played ?? null,
    wins: row.wins ?? null,
    draws: row.draws ?? null,
    losses: row.losses ?? null,
    goalsFor: row.goals_for ?? null,
    goalsAgainst: row.goals_against ?? null,
    goalDifference: row.goal_difference ?? null,
  }
}

function normalizeStreakType(raw: string | undefined): LeagueTopStreak["streakType"] | null {
  if (raw === "goal" || raw === "goal_streaks") return "goal_streaks"
  if (raw === "result" || raw === "result_streaks") return "result_streaks"
  return null
}

function normalizeScope(raw: string | null | undefined): LeagueTopStreak["scope"] {
  if (raw === "home" || raw === "away") return raw
  return "overall"
}

function toLeagueTopStreak(row: BackendTopStreakRow): LeagueTopStreak | null {
  const streakType = normalizeStreakType(row.streak_type)
  if (!streakType) return null
  const streakId = String(row.streak_id || "").trim()
  const teamId = String(row.team_id || "").trim()
  const metricKey = String(row.metric_key || row.event_name || "").trim()
  const teamName = String(row.team_name || "").trim()
  const leagueName = String(row.league_name || "").trim()
  const currentStreakLength = Number(row.current_streak_length)
  if (!streakId || !teamId || !metricKey || !teamName || !leagueName || !Number.isFinite(currentStreakLength)) {
    return null
  }

  return {
    streakId,
    metricKey,
    headline: String(row.headline || "").trim() || `${currentStreakLength} ${metricKey}`,
    streakType,
    currentStreakLength,
    teamId,
    teamName,
    teamLogoUrl: rewriteBackendAssetUrl(row.team_logo_url || undefined),
    leagueName,
    countryName: row.country_name || undefined,
    countryLogoUrl: rewriteBackendAssetUrl(row.country_logo_url || undefined),
    leagueLogoUrl: rewriteBackendAssetUrl(row.league_logo_url || undefined),
    nextKickoffUtc: row.next_kickoff_utc || undefined,
    nextOpponentName: row.next_opponent_name || undefined,
    nextFixtureId:
      row.next_fixture_id !== null && row.next_fixture_id !== undefined ? String(row.next_fixture_id) : undefined,
    nextIsHome: typeof row.next_is_home === "boolean" ? row.next_is_home : undefined,
    scope: normalizeScope(row.context_scope),
  }
}

export async function fetchLeaguePageViewModelBySlug(slug: string): Promise<LeagueAdapterResult> {
  if (!BACKEND_API_BASE_URL) {
    return {
      ok: false,
      kind: "error",
      status: 500,
      code: "backend_not_configured",
      error: "BACKEND_API_BASE_URL is not configured",
    }
  }

  try {
    const cached = await getOrSetRedisJson<BackendLeaguePageResponse>(
      redisCacheKey("league-page", slug),
      LEAGUE_PAGE_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(`${BACKEND_API_BASE_URL}/leagues/page-data?league_slug=${encodeURIComponent(slug)}`, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        })

        if (response.status === 404) {
          const details = await response.text()
          const error = new Error(details || "League page data not found")
          ;(error as Error & { status?: number; code?: string }).status = 404
          ;(error as Error & { status?: number; code?: string }).code = "league_not_found"
          throw error
        }

        if (!response.ok) {
          const details = await response.text()
          const error = new Error(details || "Backend league endpoint failed")
          ;(error as Error & { status?: number; code?: string }).status = response.status >= 500 ? 502 : response.status
          ;(error as Error & { status?: number; code?: string }).code = "backend_error"
          throw error
        }

        return response.json()
      },
    )

    const payload = cached.value
    const league = payload.data?.league
    const overall = Array.isArray(payload.data?.standings?.overall) ? payload.data!.standings!.overall!.map(toTableRow) : []
    const topStreaks = Array.isArray(payload.data?.top_streaks)
      ? payload.data!.top_streaks!.map(toLeagueTopStreak).filter((item): item is LeagueTopStreak => item !== null)
      : []

    if (!league?.league_slug || !league.league_name) {
      return {
        ok: false,
        kind: "error",
        status: 502,
        code: "invalid_payload",
        error: "League payload is missing required fields",
      }
    }

    const availableContexts: LeagueContextFilter[] = Array.isArray(payload.data?.available_contexts)
      ? payload.data!.available_contexts!
          .filter((v): v is LeagueContextFilter => v === "overall" || v === "home" || v === "away")
      : ["overall"]

    return {
      ok: true,
      source: "backend",
      data: {
        leagueId: String(league.league_slug),
        leagueName: String(league.league_name),
        leagueLogo: rewriteBackendAssetUrl(league.league_logo_url || undefined),
        countryLogo: rewriteBackendAssetUrl(league.country_logo_url || undefined),
        countryName: league.country_name || undefined,
        season: String(league.season_label || ""),
        topStreaks,
        standings: { overall },
        availableContexts: availableContexts.length > 0 ? availableContexts : ["overall"],
      },
      cache: cached.meta,
    }
  } catch (error) {
    const status = error instanceof Error ? (error as Error & { status?: number }).status : undefined
    const code = error instanceof Error ? (error as Error & { code?: string }).code : undefined
    if (status === 404 || code === "league_not_found") {
      return {
        ok: false,
        kind: "not_found",
        status: 404,
        code: "league_not_found",
        error: "League page data not found",
        details: error instanceof Error ? error.message : String(error),
      }
    }

    return {
      ok: false,
      kind: "error",
      status: status || 503,
      code: code || "adapter_error",
      error: code === "backend_error" ? "Backend league endpoint failed" : "Failed to fetch league page",
      details: error instanceof Error ? error.message : String(error),
    }
  }
}
