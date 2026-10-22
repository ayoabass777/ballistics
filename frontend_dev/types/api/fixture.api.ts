/**
 * Raw API types for fixture preview endpoint
 * These represent what the backend returns (snake_case convention)
 */

import type { MatchResult, StreakScope, StreakPolarity } from "../common"
import type { ApiMeta } from "./common"

/**
 * Raw fixture identity from backend
 */
export interface ApiFixtureIdentity {
  fixture_id: string
  slug?: string
}

/**
 * Average goals stats structure
 */
export interface ApiAvgGoalsStats {
  scored: number
  conceded: number
}

/**
 * Raw fixture team from backend
 */
export interface ApiFixtureTeam {
  team_id: string
  name: string
  short_name?: string
  slug: string
  logo_url: string
  primary_color?: string

  // Stats
  avg_goals_scored: number
  avg_goals_conceded: number
  avg_goals?: {
    overall: ApiAvgGoalsStats
    home: ApiAvgGoalsStats
    away: ApiAvgGoalsStats
  }

  // Form
  recent_form?: MatchResult[]
  form_by_context?: {
    overall: MatchResult[]
    home: MatchResult[]
    away: MatchResult[]
  }
}

/**
 * Raw fixture streak from backend
 */
export interface ApiFixtureStreak {
  event: string // Human-readable event name
  count: number // Current streak length
  total?: number // Total matches in scope
  scope: StreakScope
  polarity?: StreakPolarity
}

/**
 * Raw H2H match from backend
 */
export interface ApiH2HMatch {
  date_iso: string
  home_score: number
  away_score: number
  result: "H" | "D" | "A"
  is_home_team_home: boolean
}

/**
 * Raw H2H streak from backend
 */
export interface ApiH2HStreak {
  event: string
  count: number
  total?: number
  teams: "home" | "away" | "both"
}

/**
 * Raw form match from backend
 */
export interface ApiFormMatch {
  date_iso: string
  opponent: string
  score: string
  result: MatchResult
  is_home: boolean
}

/**
 * GET /api/fixture/:id
 * Match preview with H2H, form, and streaks
 */
export interface FixtureApiResponse {
  data: {
    fixture: ApiFixtureIdentity

    league: {
      league_season_id: string
      name: string
      logo_url: string
      country: string
      country_logo_url?: string
      matchday?: number
    }

    kickoff: {
      iso: string
    }

    venue: {
      name: string
      city: string
      capacity?: number
      image_url?: string
    }

    home_team: ApiFixtureTeam
    away_team: ApiFixtureTeam

    home_streaks: ApiFixtureStreak[]
    away_streaks: ApiFixtureStreak[]

    h2h_stats: {
      total_matches: number
      home_wins: number
      draws: number
      away_wins: number
      recent_matches: ApiH2HMatch[]
      streaks: ApiH2HStreak[]
    }

    home_team_form: ApiFormMatch[]
    away_team_form: ApiFormMatch[]
  }
  meta: ApiMeta
}
