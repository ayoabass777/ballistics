// Fixture Page (MVP) — single view model contract for pre-match analysis
// Goal: context for ONE fixture (H2H, form, streaks), no predictions, no live data.

// ============================================================================
// FIXTURE PAGE VIEW MODEL
// ============================================================================

export type FixturePageViewModel = {
  // Fixture identity
  fixture: FixtureIdentity

  // League/season context
  league: LeagueContext

  // Kickoff information
  kickoff: KickoffInfo

  // Venue details
  venue: VenueInfo

  // Team identities with stats
  homeTeam: FixtureTeam
  awayTeam: FixtureTeam

  // Team streaks heading into the match
  homeStreaks: FixtureStreak[]
  awayStreaks: FixtureStreak[]

  // Head-to-head statistics
  h2hStats: H2HStats

  // Recent form for both teams
  homeTeamForm: FormMatch[]
  awayTeamForm: FormMatch[]

  // Top-level CTAs for the page shell
  ctas: {
    backHref?: string
    shareHref?: string
    homeTeamHref: string
    awayTeamHref: string
  }

  // Metadata
  generatedAtIso: string
}

// ============================================================================
// SUPPORTING TYPES
// ============================================================================

/**
 * Core fixture identity
 */
export type FixtureIdentity = {
  fixtureId: string
  slug?: string // URL-friendly identifier
}

/**
 * League and season context for the fixture
 */
export type LeagueContext = {
  leagueSeasonId: string // Combined identifier (e.g., "premier-league-2025-26")
  name: string
  logoUrl: string
  country: string
  countryLogoUrl?: string
  matchday?: number
}

/**
 * Kickoff time information
 * Frontend handles conversion to user's local timezone
 */
export type KickoffInfo = {
  iso: string // Full ISO 8601 datetime (e.g., "2026-01-15T20:00:00Z")
}

/**
 * Venue information
 */
export type VenueInfo = {
  name: string
  city: string
  capacity?: number
  imageUrl?: string
}

/**
 * Average goals stats by context
 */
export type AvgGoalsStats = {
  scored: number
  conceded: number
}

/**
 * Team identity and stats for fixture context
 */
export type FixtureTeam = {
  teamId: string
  name: string
  shortName?: string
  slug: string // For navigation to team page
  logoUrl: string
  primaryColor?: string

  // Average goals stats for the "Attack vs Defense" section (legacy - overall)
  avgGoalsScored: number
  avgGoalsConceded: number

  // Average goals by context (for context filter)
  avgGoals?: {
    overall: AvgGoalsStats
    home: AvgGoalsStats
    away: AvgGoalsStats
  }

  // Recent form results (W/D/L) for last 5 matches
  recentForm?: Array<"W" | "D" | "L">

  // Recent form by context
  formByContext?: {
    overall: Array<"W" | "D" | "L">
    home: Array<"W" | "D" | "L">
    away: Array<"W" | "D" | "L">
  }
}

/**
 * Polarity indicates whether the streak is positive or negative for the team
 * Used for subtle ambient visual treatment (not color blocking)
 * - positive: wins, clean sheets, scoring goals, unbeaten runs
 * - negative: losses, winless runs, conceding goals, goalless runs
 * - neutral: draws, specific stat patterns with no clear good/bad (e.g., BTTS)
 */
export type StreakPolarity = "positive" | "negative" | "neutral"

/**
 * Streak item in fixture context
 * Simpler than team page streaks - focused on display
 */
export type FixtureStreak = {
  event: string // Human-readable event name (e.g., "Won at home", "Scored 2+ goals")
  count: number // Current streak length
  total?: number // Total matches in scope (e.g., "4 of last 10" → count=4, total=10)
  scope: StreakScope // "overall" | "home" | "away"
  polarity?: StreakPolarity // Subtle ambient visual treatment
}

/**
 * Scope for streaks and form
 */
export type StreakScope = "overall" | "home" | "away"

// ============================================================================
// HEAD-TO-HEAD TYPES
// ============================================================================

/**
 * Head-to-head statistics between two teams
 */
export type H2HStats = {
  totalMatches: number
  homeWins: number // Current home team wins in H2H
  draws: number
  awayWins: number // Current away team wins in H2H

  // Recent meetings list
  recentMatches: H2HMatch[]

  // H2H-specific streaks
  streaks: H2HStreak[]
}

/**
 * Individual H2H match result
 */
export type H2HMatch = {
  date: string // ISO date
  home: number // Home team score
  away: number // Away team score
  result: "H" | "D" | "A" // Home win, Draw, Away win
  isHomeTeamHome: boolean // Was current fixture's home team at home in this match?
}

/**
 * H2H-specific streak
 */
export type H2HStreak = {
  event: string // e.g., "Both teams scored"
  count: number
  total?: number
  teams: "home" | "away" | "both" // Which team(s) the streak applies to
}

// ============================================================================
// FORM TYPES
// ============================================================================

/**
 * Individual form match result
 */
export type FormMatch = {
  date: string // ISO date
  opponent: string // Opponent name
  score: string // e.g., "2-1"
  result: "W" | "L" | "D"
  isHome: boolean
}

/**
 * Match result type
 */
export type MatchResult = "W" | "D" | "L"

// ============================================================================
// API RESPONSE TYPES
// ============================================================================

/**
 * API response wrapper for fixture data
 */
export type FixtureApiResponse = {
  data: FixturePageViewModel
  meta: {
    fetchedAt: string
    cacheExpiry?: string
  }
}

/**
 * API error response
 */
export type FixtureApiError = {
  error: string
  code: string
  details?: string
}

// ============================================================================
// COMPONENT PROP TYPES
// ============================================================================

/**
 * Props for FixtureMatchupHero component
 */
export type FixtureMatchupHeroProps = {
  fixture: {
    league: Pick<LeagueContext, "name" | "logoUrl" | "matchday">
    kickoff: KickoffInfo
    venue: Pick<VenueInfo, "name" | "city" | "imageUrl">
    homeTeam: Pick<FixtureTeam, "name" | "slug" | "logoUrl">
    awayTeam: Pick<FixtureTeam, "name" | "slug" | "logoUrl">
  }
}

/**
 * Props for TeamAvgGoals component
 */
export type TeamAvgGoalsProps = {
  homeTeam: Pick<FixtureTeam, "name" | "logoUrl" | "avgGoalsScored" | "avgGoalsConceded">
  awayTeam: Pick<FixtureTeam, "name" | "logoUrl" | "avgGoalsScored" | "avgGoalsConceded">
}

/**
 * Props for H2HOverview component
 */
export type H2HOverviewProps = {
  h2hStats: Pick<H2HStats, "homeWins" | "draws" | "awayWins" | "totalMatches">
  homeTeam: Pick<FixtureTeam, "name" | "logoUrl">
  awayTeam: Pick<FixtureTeam, "name" | "logoUrl">
}

/**
 * Props for StreakItem component
 */
export type StreakItemProps = {
  streak: FixtureStreak
  team: Pick<FixtureTeam, "name" | "logoUrl">
  index: number
}

/**
 * Props for H2HStreakItem component
 */
export type H2HStreakItemProps = {
  streak: H2HStreak
  index: number
  homeTeam: Pick<FixtureTeam, "logoUrl">
  awayTeam: Pick<FixtureTeam, "logoUrl">
}

/**
 * Props for RecentMeetings component
 */
export type RecentMeetingsProps = {
  recentMatches: H2HMatch[]
  homeTeamForm: FormMatch[]
  awayTeamForm: FormMatch[]
  homeTeam: Pick<FixtureTeam, "name" | "logoUrl">
  awayTeam: Pick<FixtureTeam, "name" | "logoUrl">
}
