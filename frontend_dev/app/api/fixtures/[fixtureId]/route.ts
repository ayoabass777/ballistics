import { NextRequest, NextResponse } from "next/server"
import { mockFixture } from "@/lib/mock-fixture-data"
import { transformFixtureResponse } from "@/lib/transformers/fixture.transformers"
import type { FixtureApiResponse } from "@/types/api/fixture.api"
import { rewriteAssetUrlToFrontend } from "@/lib/logo-asset-url"
import { getOrSetRedisJson, redisCacheHeader, redisCacheKey, redisCacheTtl } from "@/lib/server/redis-cache"

const BACKEND_API_BASE_URL = process.env.BACKEND_API_BASE_URL?.replace(/\/$/, "")
const FIXTURE_PAGE_CACHE_TTL_SECONDS = redisCacheTtl("FRONTEND_FIXTURE_PAGE_CACHE_TTL_SECONDS", 120)

type RouteParams = {
  params: Promise<{ fixtureId: string }>
}

function errorPayload(code: string, error: string, fixtureId: string, details?: string) {
  return {
    code,
    error,
    fixture_id: fixtureId,
    ...(details ? { details } : {}),
  }
}

function rewriteBackendAssetUrl(url: string | undefined | null): string | undefined {
  return rewriteAssetUrlToFrontend(url)
}

function rewriteViewModelAssetUrls(viewModel: ReturnType<typeof transformFixtureResponse>) {
  return {
    ...viewModel,
    league: {
      ...viewModel.league,
      logoUrl: rewriteBackendAssetUrl(viewModel.league.logoUrl) || viewModel.league.logoUrl,
      countryLogoUrl:
        rewriteBackendAssetUrl(viewModel.league.countryLogoUrl) || viewModel.league.countryLogoUrl,
    },
    venue: {
      ...viewModel.venue,
      imageUrl: rewriteBackendAssetUrl(viewModel.venue.imageUrl) || viewModel.venue.imageUrl,
    },
    homeTeam: {
      ...viewModel.homeTeam,
      logoUrl: rewriteBackendAssetUrl(viewModel.homeTeam.logoUrl) || viewModel.homeTeam.logoUrl,
    },
    awayTeam: {
      ...viewModel.awayTeam,
      logoUrl: rewriteBackendAssetUrl(viewModel.awayTeam.logoUrl) || viewModel.awayTeam.logoUrl,
    },
  }
}

export async function GET(_request: NextRequest, { params }: RouteParams) {
  const { fixtureId } = await params
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"

  if (useMockData) {
    return NextResponse.json(mockFixture, { headers: { "x-data-source": "mock" } })
  }

  const numericFixtureId = Number(fixtureId)
  if (!Number.isFinite(numericFixtureId) || numericFixtureId <= 0) {
    return NextResponse.json(
      errorPayload("invalid_fixture_id", "Fixture ID must be a positive number", fixtureId),
      { status: 400, headers: { "x-data-source": "unavailable" } },
    )
  }

  if (!BACKEND_API_BASE_URL) {
    return NextResponse.json(
      errorPayload("backend_not_configured", "BACKEND_API_BASE_URL is not configured", fixtureId),
      { status: 500, headers: { "x-data-source": "unavailable" } },
    )
  }

  try {
    const backendUrl = `${BACKEND_API_BASE_URL}/fixtures/page-data?fixture_id=${encodeURIComponent(fixtureId)}`
    const cached = await getOrSetRedisJson<FixtureApiResponse>(
      redisCacheKey("fixture-page", fixtureId),
      FIXTURE_PAGE_CACHE_TTL_SECONDS,
      async () => {
        const response = await fetch(backendUrl, {
          headers: { Accept: "application/json" },
          cache: "no-store",
        })

        if (response.status === 404) {
          const details = await response.text()
          const error = new Error(details || "Fixture analysis is not available for this fixture yet")
          ;(error as Error & { status?: number }).status = 404
          throw error
        }

        if (!response.ok) {
          const details = await response.text()
          const error = new Error(details || "Backend fixture endpoint failed")
          ;(error as Error & { status?: number }).status = response.status >= 500 ? 502 : response.status
          throw error
        }

        return response.json()
      },
    )

    const backendPayload = cached.value
    const viewModel = rewriteViewModelAssetUrls(transformFixtureResponse(backendPayload))
    return NextResponse.json(viewModel, {
      headers: { "x-data-source": "backend", "x-cache": redisCacheHeader(cached.meta) },
    })
  } catch (error) {
    console.error("Error in fixtures API adapter:", error)
    const status = error instanceof Error ? (error as Error & { status?: number }).status : undefined
    if (status === 404) {
      return NextResponse.json(
        errorPayload("fixture_not_found", "Fixture analysis is not available for this fixture yet", fixtureId, String(error)),
        { status: 404, headers: { "x-data-source": "unavailable" } },
      )
    }

    if (status) {
      return NextResponse.json(
        errorPayload("backend_error", "Backend fixture endpoint failed", fixtureId, String(error)),
        { status, headers: { "x-data-source": "unavailable" } },
      )
    }

    return NextResponse.json(
      errorPayload("adapter_error", "Failed to fetch fixture analysis", fixtureId),
      { status: 503, headers: { "x-data-source": "unavailable" } },
    )
  }
}
