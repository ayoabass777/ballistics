import { getTeamBySlugResult } from "@/lib/data/team-data"
import { TeamPageShell } from "@/components/team/TeamPageShell"
import { TeamUnavailableState } from "@/components/team/TeamUnavailableState"

interface PageProps {
  params: Promise<{ slug: string }>
  searchParams: Promise<{ returnTo?: string }>
}

export default async function TeamPage({ params, searchParams }: PageProps) {
  const { slug } = await params
  const { returnTo } = await searchParams
  const decodedReturnTo = returnTo ? decodeURIComponent(returnTo) : null
  const result = await getTeamBySlugResult(slug)

  if (!result.ok) {
    return (
      <TeamUnavailableState
        teamSlug={slug}
        returnTo={decodedReturnTo}
        message={result.kind === "not_found" ? undefined : result.error}
      />
    )
  }

  return <TeamPageShell viewModel={result.data} returnTo={decodedReturnTo} />
}
