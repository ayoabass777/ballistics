/**
 * API Types - Re-exports all raw API response types
 * Import from "@/types/api" for all backend contract types
 */

// Common API structures
export type {
  ApiPagination,
  ApiMeta,
  ApiError,
  ApiPaginatedResponse,
  ApiResponse,
} from "./common"

// Streak API types
export type {
  ApiStreak,
  ApiLeagueOption,
  ApiStreakTypeOption,
  HomeStreaksApiResponse,
  ExploreStreaksApiResponse,
} from "./streaks.api"

// Team API types
export type {
  ApiTeamIdentity,
  ApiLeagueSeasonContext,
  ApiTeamStreak,
  ApiUpcomingMatch,
  TeamApiResponse,
} from "./team.api"

// Fixture API types
export type {
  ApiFixtureIdentity,
  ApiAvgGoalsStats,
  ApiFixtureTeam,
  ApiFixtureStreak,
  ApiH2HMatch,
  ApiH2HStreak,
  ApiFormMatch,
  FixtureApiResponse,
} from "./fixture.api"
