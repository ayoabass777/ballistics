/**
 * Streak API to View Model transformers
 * Converts raw backend responses (snake_case) to frontend VMs (camelCase)
 */

import type {
  ApiStreak,
  ApiLeagueOption,
  ApiStreakTypeOption,
} from "@/types/api/streaks.api"
import type {
  ExploreStreakVM,
  LeagueOption,
  StreakTypeOption,
} from "@/types/explore-page"

/**
 * Transforms raw API streak to ExploreStreakVM
 * Handles snake_case -> camelCase and data reshaping
 */
export function apiStreakToExploreVM(api: ApiStreak): ExploreStreakVM {
  return {
    streakId: String(api.streak_id),
    fixtureId: api.fixture_id,

    // Streak info
    metricKey: api.metric_key,
    headline: api.headline,
    streakType: api.streak_type,
    streakLength: api.current_streak_length,
    scope: api.scope,
    polarity: api.polarity,

    // Team info
    team: {
      teamId: api.team_id,
      name: api.team_name,
      logoUrl: api.team_logo_url,
    },

    // League info
    league: {
      leagueId: api.league_id,
      name: api.league_name,
      logoUrl: api.league_logo_url,
      country: api.country_name,
      countryLogoUrl: api.country_logo_url,
    },

    // Next match
    nextMatch: api.next_match
      ? {
          date: api.next_match.kickoff_iso,
          opponent: api.next_match.opponent_name,
        }
      : undefined,

    recentForm: api.recent_form,
  }
}

/**
 * Transforms API league option to VM
 */
export function apiLeagueOptionToVM(api: ApiLeagueOption): LeagueOption {
  return {
    id: api.slug,
    name: api.name,
    logoUrl: api.logo_url,
    country: api.country_name,
    countryLogoUrl: api.country_logo_url,
    count: api.streak_count,
  }
}

/**
 * Transforms API streak type option to VM
 */
export function apiStreakTypeOptionToVM(
  api: ApiStreakTypeOption
): StreakTypeOption {
  return {
    id: api.type,
    label: api.label,
    count: api.count,
  }
}

/**
 * Batch transform for explore page response
 */
export function transformExploreStreaksResponse(
  streaks: ApiStreak[],
  leagues: ApiLeagueOption[],
  types: ApiStreakTypeOption[]
) {
  return {
    streaks: streaks.map(apiStreakToExploreVM),
    filters: {
      leagues: leagues.map(apiLeagueOptionToVM),
      types: types.map(apiStreakTypeOptionToVM),
    },
  }
}

/**
 * Batch transform for home page streaks
 */
export function transformHomeStreaksResponse(streaks: ApiStreak[]) {
  return streaks.map(apiStreakToExploreVM)
}
