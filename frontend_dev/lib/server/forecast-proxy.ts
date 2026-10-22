import { auth } from "@clerk/nextjs/server"
import { NextRequest, NextResponse } from "next/server"
import { z } from "zod"

/** Fixed-origin server adapter: no mock fallback, no secrets exposed to clients. */
export async function forecastProxy(request: NextRequest, path: string, schema: z.ZodTypeAny, allowed: string[]) {
  const headers = { "Cache-Control": "no-store" }
  const fail = (status: number, error: string) => NextResponse.json({ error }, { status, headers })
  try {
    const { userId } = await auth()
    if (!userId) return fail(401, "Sign in to view the forecast record.")
    const origin = process.env.FORECAST_API_BASE_URL
    if (!origin) return fail(503, "The forecast record is not connected yet.")
    const url = new URL(origin)
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
      return fail(503, "The forecast record is not available.")
    }
    url.pathname = `${url.pathname.replace(/\/$/, "")}/api/forecast-record${path}`
    for (const key of allowed) {
      const value = request.nextUrl.searchParams.get(key)
      if (value) url.searchParams.set(key, value)
    }
    const upstream = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(8000),
      headers: { Accept: "application/json" } })
    if (!upstream.ok) return fail(upstream.status === 404 ? 404 : upstream.status === 422 ? 422 : 503,
      upstream.status === 422 ? "Please check the dates and filters." : "The forecast record is temporarily unavailable.")
    const result = schema.safeParse(await upstream.json())
    if (!result.success) return fail(502, "The forecast response could not be verified. Please try again.")
    return NextResponse.json(result.data, { headers })
  } catch {
    return fail(503, "The forecast record is temporarily unavailable. Please try again.")
  }
}
