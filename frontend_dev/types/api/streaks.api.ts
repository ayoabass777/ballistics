/**
 * Raw API types for streak endpoints
 * These represent what the backend returns (snake_case convention)
 */

import type { StreakType, StreakScope, StreakPolarity, MatchResult } from "../common"
import type { ApiPagination, ApiMeta } from "./common"

/**
 * Raw streak data from backend
 * Represents the canonical DB structure
 */
export interface ApiStreak {
  // Identifiers
  streak_id: number
  team_id: string
  fixture_id?: string // Next match fixture ID for navigation

  // Streak classification
  metric_key: string // Canonical DB key (e.g., "win", "score_2goals", "clean_sheet")
  headline: string // Pre-computed display text (e.g., "5 Consecutive Wins")
  streak_type: StreakType
  polarity: StreakPolarity
  scope: StreakScope

  // Streak data
  current_streak_length: number

  // Team context
  team_name: string
  team_slug: string
  team_logo_url?: string

  // League context
  league_id: string
  league_name: string
  league_logo_url?: string
  country_name?: string
  country_logo_url?: string

  // Next match context (optional)
  next_match?: {
    kickoff_iso: string
    opponent_name: string
    opponent_slug?: string
  }

  // Recent form (optional, for cards that show form strip)
  recent_form?: MatchResult[]

  // Bayesian model outputs (optional — omit when not yet computed)
  confidence_score?: number // final_score [0, 1] from the Bayesian form pipeline
  opponent_tier?: number    // Elo tercile of next opponent: 1=Weak, 2=Mid, 3=Strong
}

/**
 * League option from backend (for filter dropdowns)
 */
export interface ApiLeagueOption {
  league_id: string
  name: string
  slug: string
  logo_url?: string
  country_name?: string
  country_logo_url?: string
  streak_count: number // Number of active streaks in this league
}

/**
 * Streak type option from backend (for filter pills)
 */
export interface ApiStreakTypeOption {
  type: StreakType
  label: string // "Results" | "Goals"
  count: number
}

// ============================================================================
// ENDPOINT RESPONSES
// ============================================================================

/**
 * GET /api/home/streaks
 * Curated top 20 streaks for home page
 */
export interface HomeStreaksApiResponse {
  data: {
    streaks: ApiStreak[] // Top 20 curated streaks
  }
  meta: ApiMeta
}

/**
 * GET /api/explore/streaks
 * All streaks with filtering and pagination
 *
 * Query params:
 * - type?: "goal_streaks" | "result_streaks" | "all"
 * - league?: string (league slug)
 * - search?: string
 * - polarity?: "positive" | "negative" | "neutral"
 * - offset?: number (default: 0)
 * - limit?: number (default: 20, max: 50)
 */
export interface ExploreStreaksApiResponse {
  data: {
    streaks: ApiStreak[]
    filters: {
      leagues: ApiLeagueOption[]
      types: ApiStreakTypeOption[]
    }
  }
  pagination: ApiPagination
  meta: ApiMeta
}
