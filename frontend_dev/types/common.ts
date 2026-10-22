/**
 * Shared primitive types used across API and View Model types
 * These are the foundational types for the Ballistics streak system
 */

// Match result outcome
export type MatchResult = "W" | "D" | "L"

// Streak scope - where the streak is tracked
export type StreakScope = "overall" | "home" | "away"

// Streak type categories (MVP: only result and goal)
export type StreakType = "goal_streaks" | "result_streaks"

// Streak polarity - whether the streak is good or bad for the team
export type StreakPolarity = "positive" | "negative" | "neutral"

// Streak category for UI grouping
export type StreakCategory = "result" | "goals"

/**
 * League filter used in UI dropdowns
 */
export interface LeagueFilter {
  id: string
  name: string
  logo: string | null
}
