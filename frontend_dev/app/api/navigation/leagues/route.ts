import { NextResponse } from "next/server"
import { buildFallbackHomeNavResponse } from "@/lib/home-navigation"
import type { HomeNavResponse, NavCountryGroup, NavLeagueOption } from "@/types/navigation"
import { rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheHeader, redisCacheKey, redisCacheTtl } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const NAVIGATION_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_NAVIGATION_CACHE_TTL_SECONDS", 300)

type BackendNavLeague = {
  league_season_id: number
  league_name: string
  league_slug: string
  country_name: string
  league_logo_url?: string | null
  country_logo_url?: string | null
  href?: string | null
}

type BackendNavigationResponse = {
  data?: {
    leagues?: BackendNavLeague[]
  }
  meta?: {
    generatedAtIso?: string
    source?: string
    deduped?: boolean
    fallbackSlugCount?: number
  }
}

function rewriteBackendAssetUrl(url: string | undefined | null): string | null {
  return rewriteAssetUrlToFrontend(url) || null
}

function toGroupedLeagueResponse(payload: BackendNavigationResponse): HomeNavResponse | null {
  const leagues = payload.data?.leagues
  if (!Array.isArray(leagues)) return null

  type DedupedLeagueRow = {
    country: string
    countryLogoUrl: string | null
    league: NavLeagueOption
  }

  // UI-side dedupe keyed by visible league label to prevent repeated seasons from rendering duplicates.
  const dedupedByLabel = new Map<string, DedupedLeagueRow>()
  for (const row of leagues) {
    const country = row.country_name?.trim()
    const leagueName = row.league_name?.trim()
    const leagueSlug = row.league_slug?.trim()

    if (!country || !leagueName || !leagueSlug) continue

    const league: NavLeagueOption = {
      id: leagueSlug,
      name: leagueName,
      logo: rewriteBackendAssetUrl(row.league_logo_url),
      href: row.href?.trim() || `/league/${leagueSlug}`,
      leagueSeasonId: Number.isFinite(row.league_season_id) ? row.league_season_id : null,
    }

    const labelKey = `${country.toLowerCase()}::${leagueName.toLowerCase()}`
    const existing = dedupedByLabel.get(labelKey)
    if (!existing) {
      dedupedByLabel.set(labelKey, {
        country,
        countryLogoUrl: rewriteBackendAssetUrl(row.country_logo_url),
        league,
      })
      continue
    }

    const currentSeasonId = league.leagueSeasonId ?? -1
    const existingSeasonId = existing.league.leagueSeasonId ?? -1
    if (currentSeasonId > existingSeasonId) {
      dedupedByLabel.set(labelKey, {
        country,
        countryLogoUrl: rewriteBackendAssetUrl(row.country_logo_url) || existing.countryLogoUrl,
        league,
      })
    } else if (!existing.countryLogoUrl) {
      existing.countryLogoUrl = rewriteBackendAssetUrl(row.country_logo_url)
    }
  }

  const groups = new Map<string, NavCountryGroup>()
  for (const deduped of dedupedByLabel.values()) {
    const { country, countryLogoUrl, league } = deduped
    const existing = groups.get(country)
    if (existing) {
      existing.leagues.push(league)
      if (!existing.countryLogoUrl) {
        existing.countryLogoUrl = countryLogoUrl
      }
      continue
    }

    groups.set(country, {
      country,
      countryLogoUrl,
      leagues: [league],
    })
  }

  const leagueGroups = Array.from(groups.values())
    .map((group) => ({
      ...group,
      leagues: group.leagues.sort((a, b) => a.name.localeCompare(b.name)),
    }))
    .sort((a, b) => a.country.localeCompare(b.country))

  return {
    leagueGroups,
    meta: {
      source: "backend",
      fallback: false,
      generatedAtIso: payload.meta?.generatedAtIso,
      backendSource: payload.meta?.source,
      deduped: payload.meta?.deduped,
      fallbackSlugCount: payload.meta?.fallbackSlugCount,
    },
  }
}

export async function GET() {
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"
  if (useMockData) {
    return NextResponse.json(buildFallbackHomeNavResponse(), {
      headers: { "x-data-source": "mock" },
    })
  }

  if (!BACKEND_API_BASE_URL) {
    return NextResponse.json(buildFallbackHomeNavResponse("BACKEND_API_BASE_URL is not configured"), {
      headers: { "x-data-source": "mock-fallback" },
    })
  }

  try {
    const cached = await getOrSetRedisJson<BackendNavigationResponse>(
      redisCacheKey("navigation-leagues"),
      NAVIGATION_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(`${BACKEND_API_BASE_URL}/navigation/leagues`, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        })

        if (!response.ok) {
          const details = await response.text()
          throw new Error(`Backend navigation endpoint failed: ${details}`)
        }

        return response.json()
      },
    )

    const payload = cached.value
    const grouped = toGroupedLeagueResponse(payload)
    if (!grouped || grouped.leagueGroups.length === 0) {
      return NextResponse.json(buildFallbackHomeNavResponse("Malformed navigation payload"), {
        headers: { "x-data-source": "mock-fallback" },
      })
    }

    return NextResponse.json(grouped, {
      headers: { "x-data-source": "backend", "x-cache": redisCacheHeader(cached.meta) },
    })
  } catch (error) {
    console.error("Error in navigation leagues adapter:", error)
    return NextResponse.json(buildFallbackHomeNavResponse(error instanceof Error ? error.message : "Failed to fetch navigation leagues"), {
      headers: { "x-data-source": "mock-fallback" },
    })
  }
}
