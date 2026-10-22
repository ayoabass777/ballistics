import { NextRequest } from "next/server"
import { forecastProxy } from "@/lib/server/forecast-proxy"
import { recordSchema } from "@/lib/forecast-record"
export const dynamic = "force-dynamic"
export function GET(request: NextRequest) {
  return forecastProxy(request, "", recordSchema,
    ["date_from", "date_to", "team", "league", "country", "outcome", "limit", "offset", "as_of"])
}
