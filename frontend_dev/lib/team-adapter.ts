import { transformTeamResponse } from "@/lib/transformers/team.transformers"
import type { FixtureApiError } from "@/types/fixture-page"
import type { TeamApiResponse } from "@/types/api/team.api"
import type { TeamPageViewModel } from "@/types/team-page"
import { rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheKey, redisCacheTtl, type RedisCacheMeta } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const TEAM_PAGE_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_TEAM_PAGE_CACHE_TTL_SECONDS", 120)

export type TeamAdapterSuccess = {
  ok: true
  source: "mock" | "backend"
  data: TeamPageViewModel
  cache?: RedisCacheMeta
}

export type TeamAdapterFailure = {
  ok: false
  kind: "not_found" | "error"
  status: number
  code: string
  error: string
  details?: string
}

export type TeamAdapterResult = TeamAdapterSuccess | TeamAdapterFailure

function rewriteBackendAssetUrl(url: string | undefined | null): string | undefined {
  return rewriteAssetUrlToFrontend(url)
}

function rewriteTeamViewModelAssets(viewModel: TeamPageViewModel): TeamPageViewModel {
  return {
    ...viewModel,
    team: {
      ...viewModel.team,
      crestUrl: rewriteBackendAssetUrl(viewModel.team.crestUrl) || viewModel.team.crestUrl,
    },
    upcomingMatch: viewModel.upcomingMatch
      ? {
          ...viewModel.upcomingMatch,
          opponent: {
            ...viewModel.upcomingMatch.opponent,
            crestUrl:
              rewriteBackendAssetUrl(viewModel.upcomingMatch.opponent.crestUrl) ||
              viewModel.upcomingMatch.opponent.crestUrl,
          },
        }
      : undefined,
  }
}

async function loadMockTeam(slug: string): Promise<TeamAdapterResult> {
  const { MOCK_TEAMS } = await import("@/lib/data/mock/team-mock-data")
  const mock = MOCK_TEAMS[slug]
  if (!mock) {
    return {
      ok: false,
      kind: "not_found",
      status: 404,
      code: "team_not_found",
      error: "Team analysis is not available for this team yet",
    }
  }

  const data: TeamPageViewModel = {
    ...mock,
    team: {
      ...mock.team,
      slug: mock.team.slug || slug,
    },
  }

  return { ok: true, source: "mock", data }
}

export async function fetchTeamViewModelBySlug(slug: string): Promise<TeamAdapterResult> {
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"
  if (useMockData) {
    return loadMockTeam(slug)
  }

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
    const backendUrl = `${BACKEND_API_BASE_URL}/teams/page-data?team_slug=${encodeURIComponent(slug)}`
    const cached = await getOrSetRedisJson<TeamApiResponse>(
      redisCacheKey("team-page", slug),
      TEAM_PAGE_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(backendUrl, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        })

        if (response.status === 404) {
          const details = await response.text()
          const error = new Error(details || "Team analysis is not available for this team yet")
          ;(error as Error & { status?: number; code?: string }).status = 404
          ;(error as Error & { status?: number; code?: string }).code = "team_not_found"
          throw error
        }

        if (!response.ok) {
          const details = await response.text()
          const error = new Error(details || "Backend team endpoint failed")
          ;(error as Error & { status?: number; code?: string }).status = response.status >= 500 ? 502 : response.status
          ;(error as Error & { status?: number; code?: string }).code = "backend_error"
          throw error
        }

        return response.json()
      },
    )

    const payload = cached.value
    const viewModel = rewriteTeamViewModelAssets(transformTeamResponse(payload))
    return {
      ok: true,
      source: "backend",
      data: {
        ...viewModel,
        team: {
          ...viewModel.team,
          slug: viewModel.team.slug || slug,
        },
      },
      cache: cached.meta,
    }
  } catch (error) {
    const status = error instanceof Error ? (error as Error & { status?: number }).status : undefined
    const code = error instanceof Error ? (error as Error & { code?: string }).code : undefined
    if (status === 404 || code === "team_not_found") {
      return {
        ok: false,
        kind: "not_found",
        status: 404,
        code: "team_not_found",
        error: "Team analysis is not available for this team yet",
        details: error instanceof Error ? error.message : String(error),
      }
    }

    return {
      ok: false,
      kind: "error",
      status: status || 503,
      code: code || "adapter_error",
      error: code === "backend_error" ? "Backend team endpoint failed" : "Failed to fetch team analysis",
      details: error instanceof Error ? error.message : String(error),
    }
  }
}

export function teamAdapterErrorPayload(result: TeamAdapterFailure): FixtureApiError & { team_slug?: string } {
  return {
    code: result.code,
    error: result.error,
    ...(result.details ? { details: result.details } : {}),
  }
}
