export type LeagueContextFilter = "overall" | "home" | "away"

export type LeagueTopStreak = {
  streakId: string
  metricKey: string
  headline: string
  streakType: "goal_streaks" | "result_streaks"
  currentStreakLength: number
  teamId: string
  teamName: string
  teamLogoUrl?: string
  leagueName: string
  countryName?: string
  countryLogoUrl?: string
  leagueLogoUrl?: string
  nextKickoffUtc?: string
  nextOpponentName?: string
  nextFixtureId?: string
  nextIsHome?: boolean
  scope?: "overall" | "home" | "away"
}

export type LeagueTableRow = {
  position?: number | null
  teamId: string
  teamName: string
  teamSlug?: string
  teamLogo?: string
  played?: number | null
  wins?: number | null
  draws?: number | null
  losses?: number | null
  goalsFor?: number | null
  goalsAgainst?: number | null
  goalDifference?: number | null
  points?: number | null
  form: Array<"W" | "D" | "L">
}

export type LeaguePageViewModel = {
  leagueId: string
  leagueName: string
  leagueLogo?: string
  countryLogo?: string
  countryName?: string
  season: string
  topStreaks: LeagueTopStreak[]
  standings: Partial<Record<LeagueContextFilter, LeagueTableRow[]>> & { overall: LeagueTableRow[] }
  availableContexts: LeagueContextFilter[]
}
