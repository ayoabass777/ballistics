import { NextResponse } from "next/server"
import { fetchTeamViewModelBySlug, teamAdapterErrorPayload } from "@/lib/team-adapter"
import { redisCacheHeader } from "@/lib/server/redis-cache"

type RouteParams = {
  params: Promise<{ slug: string }>
}

export async function GET(_request: Request, { params }: RouteParams) {
  const { slug } = await params
  const result = await fetchTeamViewModelBySlug(slug)

  if (result.ok) {
    return NextResponse.json(result.data, {
      headers: {
        "x-data-source": result.source,
        ...(result.cache ? { "x-cache": redisCacheHeader(result.cache) } : {}),
      },
    })
  }

  return NextResponse.json(
    { ...teamAdapterErrorPayload(result), team_slug: slug },
    {
      status: result.status,
      headers: { "x-data-source": "unavailable" },
    },
  )
}
