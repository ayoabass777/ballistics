import { NextResponse } from "next/server"
import type { HomeStatsResponse } from "@/types/home-stats"
import { getOrSetRedisJson, redisCacheHeader, redisCacheKey, redisCacheTtl } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const HOME_STATS_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_HOME_STATS_CACHE_TTL_SECONDS", 60)

type BackendHomeStatsResponse = {
  data?: {
    active_streaks?: number
    leagues?: number
    teams_tracked?: number
  }
  meta?: {
    generatedAtIso?: string
    source?: string
  }
}

const FALLBACK_HOME_STATS: HomeStatsResponse = {
  data: {
    activeStreaks: 500,
    leagues: 10,
    teamsTracked: 250,
  },
  meta: {
    source: "fallback-static",
    fallback: true,
  },
}

function safeNonNegativeInt(value: unknown): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(0, Math.trunc(parsed))
}

function buildFallbackHomeStats(error?: string): HomeStatsResponse {
  return {
    data: { ...FALLBACK_HOME_STATS.data },
    meta: {
      ...FALLBACK_HOME_STATS.meta,
      ...(error ? { error } : {}),
    },
  }
}

function adaptBackendPayload(payload: BackendHomeStatsResponse): HomeStatsResponse | null {
  if (!payload || typeof payload !== "object") return null

  const data = payload.data || {}
  const activeStreaks = safeNonNegativeInt(data.active_streaks)
  const leagues = safeNonNegativeInt(data.leagues)
  const teamsTracked = safeNonNegativeInt(data.teams_tracked)

  if (activeStreaks <= 0 || leagues <= 0 || teamsTracked <= 0) {
    return null
  }

  return {
    data: {
      activeStreaks,
      leagues,
      teamsTracked,
    },
    meta: {
      source: "backend",
      fallback: false,
      generatedAtIso: payload.meta?.generatedAtIso,
      backendSource: payload.meta?.source,
    },
  }
}

export async function GET() {
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"
  if (useMockData) {
    return NextResponse.json(buildFallbackHomeStats(), {
      headers: { "x-data-source": "mock" },
    })
  }

  if (!BACKEND_API_BASE_URL) {
    return NextResponse.json(buildFallbackHomeStats("BACKEND_API_BASE_URL is not configured"), {
      headers: { "x-data-source": "mock-fallback" },
    })
  }

  try {
    const cached = await getOrSetRedisJson<BackendHomeStatsResponse>(
      redisCacheKey("home-stats"),
      HOME_STATS_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(`${BACKEND_API_BASE_URL}/home/stats`, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        })

        if (!response.ok) {
          const details = await response.text()
          throw new Error(`Backend home stats endpoint failed: ${details}`)
        }

        return response.json()
      },
    )

    const payload = cached.value
    const adapted = adaptBackendPayload(payload)
    if (!adapted) {
      return NextResponse.json(buildFallbackHomeStats("Malformed home stats payload"), {
        headers: { "x-data-source": "mock-fallback" },
      })
    }

    return NextResponse.json(adapted, {
      headers: { "x-data-source": "backend", "x-cache": redisCacheHeader(cached.meta) },
    })
  } catch (error) {
    console.error("Error in home stats adapter:", error)
    return NextResponse.json(buildFallbackHomeStats(error instanceof Error ? error.message : "Failed to fetch home stats"), {
      headers: { "x-data-source": "mock-fallback" },
    })
  }
}
