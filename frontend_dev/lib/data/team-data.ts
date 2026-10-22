import type { TeamPageViewModel } from "@/types/team-page"
import { fetchTeamViewModelBySlug, type TeamAdapterResult } from "@/lib/team-adapter"

/**
 * Data service interface for team page data
 * This abstraction allows easy swapping between mock and API implementations
 */
export interface TeamDataService {
  getTeamBySlug(slug: string): Promise<TeamPageViewModel | null>
}

export async function getTeamBySlugResult(slug: string): Promise<TeamAdapterResult> {
  return fetchTeamViewModelBySlug(slug)
}

/**
 * Fetches team data by slug
 * Currently uses mock data, will be replaced with API call when backend is ready
 */
export async function getTeamBySlug(slug: string): Promise<TeamPageViewModel | null> {
  const result = await fetchTeamViewModelBySlug(slug)
  return result.ok ? result.data : null
}
