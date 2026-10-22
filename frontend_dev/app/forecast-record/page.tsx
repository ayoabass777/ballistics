import { Suspense } from "react"
import type { Metadata } from "next"
import { ForecastRecordPage } from "@/components/forecast/ForecastRecordPage"
export const metadata: Metadata = { title: "Forecast record | Ballistics" }
export default function Page() {
  return <Suspense fallback={<main className="p-8" role="status">Loading forecast record…</main>}><ForecastRecordPage /></Suspense>
}
