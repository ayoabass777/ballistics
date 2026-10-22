// Explore Page (MVP) — single view model contract
// Goal: Browse all active streaks across leagues with filtering

export type ExplorePageViewModel = {
  // all active streaks
  streaks: ExploreStreakVM[]

  // available filters (populated from streaks data)
  filters: {
    leagues: LeagueOption[]
    types: StreakTypeOption[]
  }

  // metadata
  totalCount: number
  generatedAtIso: string
}

// ---------- Supporting Types ----------

export type StreakType = "goal_streaks" | "result_streaks"
export type StreakScope = "overall" | "home" | "away"
export type StreakResult = "W" | "D" | "L"
export type StreakPolarity = "positive" | "negative" | "neutral"

/**
 * Streak item for explore page
 * Includes all data needed for card display + filtering
 */
export type ExploreStreakVM = {
  // identifiers
  streakId: string
  fixtureId?: string // for navigation to fixture page

  // streak info
  metricKey: string // canonical key from DB (e.g., "win", "score_2goals", "clean_sheet")
  headline: string // human-readable display text (e.g., "5 Consecutive Wins")
  streakType: StreakType
  streakLength: number
  scope?: StreakScope
  polarity?: StreakPolarity

  // team info
  team: {
    teamId: string
    name: string
    logoUrl?: string
  }

  // league info
  league: {
    leagueId: string
    name: string
    logoUrl?: string
    country?: string
    countryLogoUrl?: string
  }

  // next match context
  nextMatch?: {
    date: string // ISO date
    opponent: string
  }

  // optional extras
  recentForm?: StreakResult[]
}

/**
 * League filter option
 */
export type LeagueOption = {
  id: string // URL-friendly slug (e.g., "premier-league")
  name: string // Display name (e.g., "Premier League")
  logoUrl?: string
  country?: string
  countryLogoUrl?: string
  count: number // Number of streaks in this league
}

/**
 * Streak type filter option
 */
export type StreakTypeOption = {
  id: StreakType
  label: string // Display label (e.g., "Results", "Goals", "Special")
  count: number // Number of streaks of this type
}

// ---------- Filter State Types ----------

/**
 * Current filter state for URL params
 */
export type ExploreFilters = {
  type?: StreakType | "all"
  league?: string // league slug or "all"
  search?: string
}

/**
 * Active filters for display (pills)
 */
export type ActiveFilters = {
  type?: string
  league?: string
  search?: string
}
