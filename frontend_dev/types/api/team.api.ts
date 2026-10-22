/**
 * Raw API types for team detail endpoint
 * These represent what the backend returns (snake_case convention)
 */

import type { MatchResult, StreakScope, StreakPolarity, StreakType } from "../common"
import type { ApiMeta } from "./common"

/**
 * Raw team identity from backend
 */
export interface ApiTeamIdentity {
  team_id: string
  name: string
  short_name?: string
  slug: string
  crest_url?: string
  rank?: number
}

/**
 * Raw league/season context from backend
 */
export interface ApiLeagueSeasonContext {
  league_season_id: string // Combined identifier (e.g., "premier-league-2025-26")
  league_name: string
  season_label: string // "2025/26"
  country_name?: string
  league_teams: Array<{
    team_id: string
    name: string
    slug: string
  }>
}

/**
 * Raw streak for team page
 */
export interface ApiTeamStreak {
  streak_id: string
  team_id: string

  // Classification
  streak_type: StreakType
  polarity: StreakPolarity
  scope?: StreakScope

  // Streak info
  metric_key: string
  threshold?: number // e.g., 1, 2, 3 (for "2+")
  length: number
  headline: string
}

/**
 * Raw upcoming match from backend
 */
export interface ApiUpcomingMatch {
  match_id: string
  kickoff_iso?: string
  is_home: boolean
  opponent: {
    team_id: string
    name: string
    slug: string
    crest_url?: string
    rank?: number
  }
  venue?: string
}

/**
 * GET /api/team/:slug
 * Team detail with form and streaks
 */
export interface TeamApiResponse {
  data: {
    team: ApiTeamIdentity
    context: ApiLeagueSeasonContext

    // Form (Last 5)
    form: {
      last_n: MatchResult[] // Last 5 outcomes
    }

    // Streaks grouped by priority
    current_streaks: {
      primary: ApiTeamStreak[] // 3-6 max
      secondary?: ApiTeamStreak[] // Optional extra row
    }

    // Next match context (if within 7 days)
    upcoming_match?: ApiUpcomingMatch

    // UI hints computed by backend
    ui_hints?: {
      show_upcoming_match: boolean
      show_secondary_streak_row: boolean
    }
  }
  meta: ApiMeta
}
