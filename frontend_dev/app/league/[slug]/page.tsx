import { notFound } from "next/navigation"
import { LeaguePageShell } from "@/components/league/LeaguePageShell"
import { getLeagueBySlugResult } from "@/lib/data/league-data"

interface PageProps {
  params: Promise<{ slug: string }>
}

export default async function LeaguePage({ params }: PageProps) {
  const { slug: rawSlug } = await params
  const slug = decodeURIComponent(rawSlug)
  const result = await getLeagueBySlugResult(slug)

  if (!result.ok) {
    if (result.kind === "not_found") {
      notFound()
    }
    notFound()
  }

  return <LeaguePageShell leagueData={result.data} />
}
