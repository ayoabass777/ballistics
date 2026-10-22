/**
 * Transformers - Re-exports all API to VM transformation functions
 * Import from "@/lib/transformers" for all transformer utilities
 */

// Streak transformers
export {
  apiStreakToExploreVM,
  apiLeagueOptionToVM,
  apiStreakTypeOptionToVM,
  transformExploreStreaksResponse,
  transformHomeStreaksResponse,
} from "./streak.transformers"

// Team transformers
export {
  apiTeamIdentityToVM,
  apiLeagueSeasonContextToVM,
  apiTeamStreakToVM,
  apiUpcomingMatchToVM,
  transformTeamResponse,
} from "./team.transformers"

// Fixture transformers
export {
  apiFixtureTeamToVM,
  apiFixtureStreakToVM,
  apiH2HMatchToVM,
  apiH2HStreakToVM,
  apiFormMatchToVM,
  transformFixtureResponse,
} from "./fixture.transformers"
