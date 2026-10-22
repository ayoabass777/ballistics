/**
 * Fixture API to View Model transformers
 * Converts raw backend responses (snake_case) to frontend VMs (camelCase)
 */

import type {
  ApiFixtureTeam,
  ApiFixtureStreak,
  ApiH2HMatch,
  ApiH2HStreak,
  ApiFormMatch,
  FixtureApiResponse,
} from "@/types/api/fixture.api"
import type {
  FixturePageViewModel,
  FixtureIdentity,
  LeagueContext,
  KickoffInfo,
  VenueInfo,
  FixtureTeam,
  FixtureStreak,
  H2HStats,
  H2HMatch,
  H2HStreak,
  FormMatch,
} from "@/types/fixture-page"

/**
 * Transforms API fixture team to VM
 */
export function apiFixtureTeamToVM(api: ApiFixtureTeam): FixtureTeam {
  return {
    teamId: api.team_id,
    name: api.name,
    shortName: api.short_name,
    slug: api.slug,
    logoUrl: api.logo_url,
    primaryColor: api.primary_color,
    avgGoalsScored: api.avg_goals_scored,
    avgGoalsConceded: api.avg_goals_conceded,
    avgGoals: api.avg_goals
      ? {
          overall: {
            scored: api.avg_goals.overall.scored,
            conceded: api.avg_goals.overall.conceded,
          },
          home: {
            scored: api.avg_goals.home.scored,
            conceded: api.avg_goals.home.conceded,
          },
          away: {
            scored: api.avg_goals.away.scored,
            conceded: api.avg_goals.away.conceded,
          },
        }
      : undefined,
    recentForm: api.recent_form,
    formByContext: api.form_by_context,
  }
}

/**
 * Transforms API fixture streak to VM
 */
export function apiFixtureStreakToVM(api: ApiFixtureStreak): FixtureStreak {
  return {
    event: api.event,
    count: api.count,
    total: api.total,
    scope: api.scope,
    polarity: api.polarity,
  }
}

/**
 * Transforms API H2H match to VM
 */
export function apiH2HMatchToVM(api: ApiH2HMatch): H2HMatch {
  return {
    date: api.date_iso,
    home: api.home_score,
    away: api.away_score,
    result: api.result,
    isHomeTeamHome: api.is_home_team_home,
  }
}

/**
 * Transforms API H2H streak to VM
 */
export function apiH2HStreakToVM(api: ApiH2HStreak): H2HStreak {
  return {
    event: api.event,
    count: api.count,
    total: api.total,
    teams: api.teams,
  }
}

/**
 * Transforms API form match to VM
 */
export function apiFormMatchToVM(api: ApiFormMatch): FormMatch {
  return {
    date: api.date_iso,
    opponent: api.opponent,
    score: api.score,
    result: api.result,
    isHome: api.is_home,
  }
}

/**
 * Transforms full fixture API response to page view model
 */
export function transformFixtureResponse(
  response: FixtureApiResponse
): FixturePageViewModel {
  const { data, meta } = response

  const fixture: FixtureIdentity = {
    fixtureId: data.fixture.fixture_id,
    slug: data.fixture.slug,
  }

  const league: LeagueContext = {
    leagueSeasonId: data.league.league_season_id,
    name: data.league.name,
    logoUrl: data.league.logo_url,
    country: data.league.country,
    countryLogoUrl: data.league.country_logo_url,
    matchday: data.league.matchday,
  }

  const kickoff: KickoffInfo = {
    iso: data.kickoff.iso,
  }

  const venue: VenueInfo = {
    name: data.venue.name,
    city: data.venue.city,
    capacity: data.venue.capacity,
    imageUrl: data.venue.image_url,
  }

  const h2hStats: H2HStats = {
    totalMatches: data.h2h_stats.total_matches,
    homeWins: data.h2h_stats.home_wins,
    draws: data.h2h_stats.draws,
    awayWins: data.h2h_stats.away_wins,
    recentMatches: data.h2h_stats.recent_matches.map(apiH2HMatchToVM),
    streaks: data.h2h_stats.streaks.map(apiH2HStreakToVM),
  }

  return {
    fixture,
    league,
    kickoff,
    venue,
    homeTeam: apiFixtureTeamToVM(data.home_team),
    awayTeam: apiFixtureTeamToVM(data.away_team),
    homeStreaks: data.home_streaks.map(apiFixtureStreakToVM),
    awayStreaks: data.away_streaks.map(apiFixtureStreakToVM),
    h2hStats,
    homeTeamForm: data.home_team_form.map(apiFormMatchToVM),
    awayTeamForm: data.away_team_form.map(apiFormMatchToVM),
    ctas: {
      homeTeamHref: `/team/${data.home_team.slug}`,
      awayTeamHref: `/team/${data.away_team.slug}`,
    },
    generatedAtIso: meta.generatedAtIso,
  }
}
