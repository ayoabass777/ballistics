/**
 * Team API to View Model transformers
 * Converts raw backend responses (snake_case) to frontend VMs (camelCase)
 */

import type {
  ApiTeamIdentity,
  ApiLeagueSeasonContext,
  ApiTeamStreak,
  ApiUpcomingMatch,
  TeamApiResponse,
} from "@/types/api/team.api"
import type {
  TeamPageViewModel,
  TeamIdentity,
  LeagueSeasonContext,
  StreakCardVM,
  UpcomingMatchVM,
} from "@/types/team-page"

/**
 * Transforms API team identity to VM
 */
export function apiTeamIdentityToVM(api: ApiTeamIdentity): TeamIdentity {
  return {
    teamId: api.team_id,
    name: api.name,
    slug: api.slug,
    shortName: api.short_name,
    crestUrl: api.crest_url,
    rank: api.rank,
  }
}

/**
 * Transforms API league/season context to VM
 */
export function apiLeagueSeasonContextToVM(
  api: ApiLeagueSeasonContext
): LeagueSeasonContext {
  return {
    leagueSeasonId: api.league_season_id,
    leagueName: api.league_name,
    seasonLabel: api.season_label,
    countryName: api.country_name,
    leagueTeams: api.league_teams.map((t) => ({
      teamId: t.team_id,
      name: t.name,
      teamHref: `/team/${t.slug}`,
    })),
  }
}

/**
 * Transforms API team streak to StreakCardVM
 */
export function apiTeamStreakToVM(api: ApiTeamStreak): StreakCardVM {
  return {
    streakId: api.streak_id,
    teamId: api.team_id,
    category: api.streak_type === "result_streaks" ? "result" : "goals",
    polarity: api.polarity,
    scope: api.scope,
    metricKey: api.metric_key,
    length: api.length,
    headline: api.headline,
  }
}

/**
 * Transforms API upcoming match to VM
 */
export function apiUpcomingMatchToVM(api: ApiUpcomingMatch): UpcomingMatchVM {
  return {
    matchId: api.match_id,
    kickoffIso: api.kickoff_iso,
    isHome: api.is_home,
    opponent: {
      teamId: api.opponent.team_id,
      name: api.opponent.name,
      crestUrl: api.opponent.crest_url,
      teamHref: `/team/${api.opponent.slug}`,
      rank: api.opponent.rank,
    },
    venue: api.venue,
  }
}

/**
 * Transforms full team API response to page view model
 */
export function transformTeamResponse(
  response: TeamApiResponse
): TeamPageViewModel {
  const { data, meta } = response

  return {
    team: apiTeamIdentityToVM(data.team),
    context: apiLeagueSeasonContextToVM(data.context),

    form: {
      lastN: data.form.last_n,
    },

    currentStreaks: {
      primary: data.current_streaks.primary.map(apiTeamStreakToVM),
      secondary: data.current_streaks.secondary?.map(apiTeamStreakToVM),
    },

    upcomingMatch: data.upcoming_match
      ? apiUpcomingMatchToVM(data.upcoming_match)
      : undefined,

    ctas: {
      backToLeagueHref: "/",
      exploreStreaksHref: "/explore",
    },

    uiHints: data.ui_hints
      ? {
          showUpcomingMatch: data.ui_hints.show_upcoming_match,
          showSecondaryStreakRow: data.ui_hints.show_secondary_streak_row,
        }
      : undefined,

    generatedAtIso: meta.generatedAtIso,
  }
}
