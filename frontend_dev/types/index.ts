/**
 * Core type definitions for the Ballistics application
 * Re-exports common types and provides legacy compatibility
 */

// Re-export common primitives
export type {
  MatchResult,
  StreakScope,
  StreakType,
  StreakPolarity,
  StreakCategory,
  LeagueFilter,
} from "./common"

// Re-export API types (for components that need raw API types)
export type {
  ApiStreak,
  ApiLeagueOption,
  ApiStreakTypeOption,
  ApiPagination,
  ApiMeta,
  ApiError,
} from "./api"

// Legacy StreakData interface (for backward compatibility with existing components)
export interface StreakData {
  team_name: string
  team_logo_url?: string
  league_name: string
  country_name?: string
  country_logo_url?: string
  league_logo_url?: string
  headline: string
  secondary_streak?: string
  recent_form?: Array<"W" | "D" | "L">
  scope?: "overall" | "home" | "away"
  next_opponent?: string
  next_match_date?: string
  next_is_home?: boolean
  fixture_id?: string
}

export interface StreakCardProps extends StreakData {
  onViewStats: () => void
  index: number
}

// Hook return types
export interface UseStreakDataReturn {
  streaks: StreakData[]
  loading: boolean
  error: string | null
  refetch: () => void
  isValidating: boolean
}

export interface UseStreakFilterReturn {
  filteredStreaks: StreakData[]
}

export interface UseStreakSearchReturn {
  searchQuery: string
  setSearchQuery: (query: string) => void
  debouncedQuery: string
  filteredStreaks: StreakData[]
}
