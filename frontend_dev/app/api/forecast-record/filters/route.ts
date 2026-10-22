import { NextRequest } from "next/server"
import { forecastProxy } from "@/lib/server/forecast-proxy"
import { directorySchema } from "@/lib/forecast-record"
export const dynamic = "force-dynamic"
export function GET(request: NextRequest) {
  return forecastProxy(request, "/filters", directorySchema, ["as_of"])
}
