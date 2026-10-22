import { NextResponse } from "next/server"
import { fetchLeaguePageViewModelBySlug } from "@/lib/league-adapter"
import { redisCacheHeader } from "@/lib/server/redis-cache"

type RouteParams = {
  params: Promise<{ slug: string }>
}

export async function GET(_request: Request, { params }: RouteParams) {
  const { slug: rawSlug } = await params
  const slug = decodeURIComponent(rawSlug)
  const result = await fetchLeaguePageViewModelBySlug(slug)

  if (result.ok) {
    return NextResponse.json(result.data, {
      headers: {
        "x-data-source": result.source,
        ...(result.cache ? { "x-cache": redisCacheHeader(result.cache) } : {}),
      },
    })
  }

  return NextResponse.json(
    {
      code: result.code,
      error: result.error,
      ...(result.details ? { details: result.details } : {}),
      league_slug: slug,
    },
    {
      status: result.status,
      headers: { "x-data-source": "unavailable" },
    },
  )
}
