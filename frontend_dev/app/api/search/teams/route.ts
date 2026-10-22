import { NextRequest, NextResponse } from "next/server"
import type { TeamSearchResponse } from "@/types/search"
import { rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheHeader, redisCacheKey, redisCacheTtl } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const DEFAULT_LIMIT = 6
const TEAM_SEARCH_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_TEAM_SEARCH_CACHE_TTL_SECONDS", 30)

type BackendSuggestion = {
  team_id: number
  name: string
  slug: string
  href: string
  logo_url?: string | null
  league_name?: string | null
  country_logo_url?: string | null
}

type BackendSearchResponse = {
  data?: { suggestions?: BackendSuggestion[] }
  meta?: {
    source?: string
    query?: string
    limit?: number
    generatedAtIso?: string
  }
}

function rewriteBackendAssetUrl(url: string | undefined | null): string | null {
  return rewriteAssetUrlToFrontend(url) || null
}

function emptyResponse(query: string, error?: string): TeamSearchResponse {
  return {
    suggestions: [],
    meta: {
      source: "search-fallback-empty",
      query,
      limit: DEFAULT_LIMIT,
      fallback: true,
      ...(error ? { error } : {}),
    },
  }
}

export async function GET(request: NextRequest) {
  const query = (request.nextUrl.searchParams.get("q") || "").trim()
  const limitParam = Number(request.nextUrl.searchParams.get("limit") || DEFAULT_LIMIT)
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(Math.trunc(limitParam), 1), 10) : DEFAULT_LIMIT

  if (query.length < 2) {
    return NextResponse.json(emptyResponse(query), { headers: { "x-data-source": "empty" } })
  }

  if (!BACKEND_API_BASE_URL) {
    return NextResponse.json(emptyResponse(query, "BACKEND_API_BASE_URL is not configured"), {
      headers: { "x-data-source": "unavailable" },
    })
  }

  try {
    const cached = await getOrSetRedisJson<BackendSearchResponse>(
      redisCacheKey("team-search", query.toLowerCase(), limit),
      TEAM_SEARCH_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(
          `${BACKEND_API_BASE_URL}/search/teams?q=${encodeURIComponent(query)}&limit=${encodeURIComponent(String(limit))}`,
          {
            headers: { Accept: "application/json" },
            cache: "no-store",
          },
        )

        if (!response.ok) {
          const details = await response.text()
          throw new Error(`Backend search failed: ${details}`)
        }

        return response.json()
      },
    )

    const payload = cached.value
    const suggestions = Array.isArray(payload.data?.suggestions)
      ? payload.data!.suggestions!.map((item) => ({
          teamId: item.team_id,
          name: item.name,
          slug: item.slug,
          href: item.href,
          logoUrl: rewriteBackendAssetUrl(item.logo_url),
          leagueName: item.league_name || null,
          countryLogoUrl: rewriteBackendAssetUrl(item.country_logo_url),
        }))
      : []

    return NextResponse.json(
      {
        suggestions,
        meta: {
          source: payload.meta?.source || "backend",
          query: payload.meta?.query || query,
          limit: payload.meta?.limit || limit,
          generatedAtIso: payload.meta?.generatedAtIso,
        },
      } satisfies TeamSearchResponse,
      { headers: { "x-data-source": "backend", "x-cache": redisCacheHeader(cached.meta) } },
    )
  } catch (error) {
    console.error("Error in team search adapter:", error)
    return NextResponse.json(emptyResponse(query, error instanceof Error ? error.message : "Failed to fetch team suggestions"), {
      status: 503,
      headers: { "x-data-source": "unavailable" },
    })
  }
}
