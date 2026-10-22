import { NextRequest, NextResponse } from "next/server"
import { forecastProxy } from "@/lib/server/forecast-proxy"
import { historySchema } from "@/lib/forecast-record"
export const dynamic = "force-dynamic"
export async function GET(request: NextRequest, { params }: { params: Promise<{ entryId: string }> }) {
  const { entryId } = await params
  if (!/^[1-9]\d*$/.test(entryId)) return NextResponse.json({ error: "Invalid forecast." }, { status: 400 })
  return forecastProxy(request, `/entries/${entryId}/history`, historySchema, ["as_of", "after_id", "limit"])
}
