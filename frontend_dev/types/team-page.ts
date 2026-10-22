// Team Page (MVP) — single view model contract
// Goal: context for ONE team (form + streaks), no predictions, no matchup analysis.

export type TeamPageViewModel = {
  // identity + routing anchors
  team: TeamIdentity
  context: LeagueSeasonContext

  // form window (fixed to Last 5 for MVP)
  // W/D/L summary + lastN outcomes (no charts)
  form: TeamFormSummary

  // headline streaks (the "drivers" shown prominently)
  currentStreaks: {
    primary: StreakCardVM[] // 3–6 max
    secondary?: StreakCardVM[] // optional extra row
  }

  // neutral next match context (optional; NO comparison)
  upcomingMatch?: UpcomingMatchVM

  // top-level CTAs for the page shell
  ctas: {
    backToLeagueHref: string
    exploreStreaksHref: string // global streaks page
  }

  // Backend-computed flags that control conditional rendering
  // These allow the backend to decide what UI elements to show based on data availability
  uiHints?: {
    showUpcomingMatch: boolean // true if upcomingMatch exists and kickoff is within 7 days
    showSecondaryStreakRow: boolean // true if secondary array has 2+ streaks
  }

  // Timestamp when the streak engine last computed this data
  // Used to show "Last updated: X ago" in the UI
  // Note: This is when streaks were calculated, NOT when fixtures were updated
  // Fixture postponements are reflected in upcomingMatch.kickoffIso
  generatedAtIso: string
}

// ---------- Supporting Types ----------

export type TeamIdentity = {
  teamId: string
  name: string
  slug?: string
  shortName?: string
  crestUrl?: string
  rank?: number
}

export type LeagueSeasonContext = {
  leagueSeasonId: string // Combined identifier (e.g., "premier-league-2025-26")
  leagueName: string
  seasonLabel: string // "2025/26"
  countryName?: string
  leagueTeams: Array<{
    teamId: string
    name: string
    teamHref: string
  }>
}


export type TeamFormSummary = {
  // outcomes only (compact strip) - Last 5
  lastN: Array<"W" | "D" | "L">
}

export type StreakCategory = "result" | "goals"

export type StreakCardVM = {
  // identifiers
  streakId: string
  teamId: string

  // grouping
  category: StreakCategory
  polarity: "positive" | "negative" | "neutral"
  scope?: "overall" | "home" | "away"

  // streak info
  metricKey: string // canonical key for deduplication/filtering (e.g., "win", "score_2", "clean_sheet")
  length: number // current streak length

  // display text (pre-computed by backend)
  headline: string // e.g. "Unbeaten in 6", "Scoring 2+ in 5 straight"
}

export type UpcomingMatchVM = {
  matchId: string
  kickoffIso?: string

  // enough to render "Upcoming match vs X" + home/away
  isHome: boolean
  opponent: {
    teamId: string
    name: string
    crestUrl?: string
    teamHref: string
    rank?: number // Added rank field for opponent
  }

  venue?: string
}
