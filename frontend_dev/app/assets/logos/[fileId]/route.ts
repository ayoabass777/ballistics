import { NextRequest, NextResponse } from "next/server"
import { backendLogoAssetUrl } from "@/lib/logo-asset-url"

type RouteParams = {
  params: Promise<{ fileId: string }>
}

export async function GET(_request: NextRequest, { params }: RouteParams) {
  const { fileId } = await params
  const backendUrl = backendLogoAssetUrl(fileId)

  if (!backendUrl) {
    return NextResponse.json({ error: "Logo asset is not configured" }, { status: 500 })
  }

  try {
    const response = await fetch(backendUrl, {
      headers: { Accept: "image/*" },
      cache: "force-cache",
    })

    if (!response.ok) {
      return new NextResponse(response.body, {
        status: response.status,
        headers: {
          "content-type": response.headers.get("content-type") || "application/octet-stream",
          "cache-control": response.headers.get("cache-control") || "public, max-age=300",
        },
      })
    }

    return new NextResponse(response.body, {
      status: 200,
      headers: {
        "content-type": response.headers.get("content-type") || "application/octet-stream",
        "cache-control": "public, max-age=86400, s-maxage=86400, stale-while-revalidate=604800",
      },
    })
  } catch (error) {
    console.error("Error proxying logo asset:", error)
    return NextResponse.json({ error: "Failed to fetch logo asset" }, { status: 502 })
  }
}
