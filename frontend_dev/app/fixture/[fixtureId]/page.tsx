import { Suspense } from "react"
import { FixtureAnalysisPageClient } from "./FixtureAnalysisPageClient"
import { FixturePageSkeleton } from "@/components/fixture/FixturePageSkeleton"

interface PageProps {
  params: Promise<{ fixtureId: string }>
  searchParams: Promise<{ returnTo?: string }>
}

export default async function FixtureAnalysisPage({ params, searchParams }: PageProps) {
  const { fixtureId } = await params
  const { returnTo } = await searchParams

  // Decode returnTo if it exists
  const decodedReturnTo = returnTo ? decodeURIComponent(returnTo) : null

  return (
    <Suspense fallback={<FixturePageSkeleton />}>
      <FixtureAnalysisPageClient fixtureId={fixtureId} returnTo={decodedReturnTo} />
    </Suspense>
  )
}
