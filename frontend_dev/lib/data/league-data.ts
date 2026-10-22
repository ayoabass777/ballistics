import { fetchLeaguePageViewModelBySlug, type LeagueAdapterResult } from "@/lib/league-adapter"

export async function getLeagueBySlugResult(slug: string): Promise<LeagueAdapterResult> {
  return fetchLeaguePageViewModelBySlug(slug)
}
