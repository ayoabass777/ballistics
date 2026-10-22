import type { FixturePageViewModel, FormMatch } from "@/types/fixture-page"

/**
 * Mock fixture data for development
 * Will be replaced with API data when backend is ready
 */
export const MOCK_FIXTURES: Record<string, FixturePageViewModel> = {
  "man-city-vs-liverpool": {
    fixture: {
      fixtureId: "man-city-vs-liverpool",
      slug: "man-city-vs-liverpool",
    },
    league: {
      leagueSeasonId: "premier-league-2025-26",
      name: "Premier League",
      logoUrl: "https://media.api-sports.io/football/leagues/39.png",
      country: "England",
      countryLogoUrl: "https://flagcdn.com/w40/gb-eng.png",
      matchday: 21,
    },
    kickoff: {
      iso: "2099-01-15T20:00:00Z",
    },
    venue: {
      name: "Etihad Stadium",
      city: "Manchester",
      capacity: 53400,
      imageUrl: "/modern-football-stadium-etihad.jpg",
    },
    homeTeam: {
      teamId: "man-city",
      name: "Manchester City",
      shortName: "MCI",
      slug: "manchester-city",
      logoUrl: "https://media.api-sports.io/football/teams/50.png",
      primaryColor: "#6CABDD",
      avgGoalsScored: 2.3,
      avgGoalsConceded: 1.1,
      avgGoals: {
        overall: { scored: 2.3, conceded: 1.1 },
        home: { scored: 2.8, conceded: 0.8 },
        away: { scored: 1.8, conceded: 1.4 },
      },
      recentForm: ["W", "D", "W", "W", "L"],
      formByContext: {
        overall: ["W", "D", "W", "W", "L"],
        home: ["W", "W", "W", "D", "W"],
        away: ["D", "W", "L", "W", "D"],
      },
    },
    awayTeam: {
      teamId: "liverpool",
      name: "Liverpool",
      shortName: "LIV",
      slug: "liverpool",
      logoUrl: "https://media.api-sports.io/football/teams/40.png",
      primaryColor: "#C8102E",
      avgGoalsScored: 2.5,
      avgGoalsConceded: 1.3,
      avgGoals: {
        overall: { scored: 2.5, conceded: 1.3 },
        home: { scored: 3.0, conceded: 1.0 },
        away: { scored: 2.0, conceded: 1.6 },
      },
      recentForm: ["W", "W", "D", "W", "W"],
      formByContext: {
        overall: ["W", "W", "D", "W", "W"],
        home: ["W", "W", "W", "D", "W"],
        away: ["W", "D", "W", "L", "W"],
      },
    },
    homeStreaks: [
      { event: "Won at home", count: 5, total: 10, scope: "home", polarity: "positive" },
      { event: "Scored 2+ goals", count: 7, total: 10, scope: "overall", polarity: "positive" },
      { event: "Kept clean sheet at home", count: 3, total: 10, scope: "home", polarity: "positive" },
      { event: "Scored first", count: 6, scope: "overall", polarity: "positive" },
    ],
    awayStreaks: [
      { event: "Unbeaten", count: 12, scope: "overall", polarity: "positive" },
      { event: "Scored 1+ goals", count: 15, scope: "overall", polarity: "positive" },
      { event: "Won away", count: 4, total: 10, scope: "away", polarity: "positive" },
      { event: "Failed to keep clean sheet", count: 5, scope: "away", polarity: "negative" },
    ],
    h2hStats: {
      totalMatches: 10,
      homeWins: 4,
      draws: 3,
      awayWins: 3,
      recentMatches: [
        { date: "2025-11-10", home: 1, away: 1, result: "D", isHomeTeamHome: true },
        { date: "2025-08-03", home: 2, away: 3, result: "A", isHomeTeamHome: false },
        { date: "2025-03-10", home: 1, away: 1, result: "D", isHomeTeamHome: true },
        { date: "2024-11-25", home: 2, away: 0, result: "H", isHomeTeamHome: true },
        { date: "2024-07-30", home: 0, away: 2, result: "A", isHomeTeamHome: false },
      ],
      streaks: [
        { event: "Both teams scored", count: 7, total: 10, teams: "both" },
        { event: "Over 2.5 goals", count: 6, total: 10, teams: "both" },
        { event: "Remained unbeaten", count: 5, total: 5, teams: "away" },
        { event: "Won at home", count: 4, total: 10, teams: "home" },
      ],
    },
    homeTeamForm: [
      { date: "2024-01-15", opponent: "Liverpool", score: "2-1", result: "W", isHome: true },
      { date: "2024-01-10", opponent: "Arsenal", score: "1-1", result: "D", isHome: false },
      { date: "2024-01-05", opponent: "Chelsea", score: "3-2", result: "W", isHome: true },
      { date: "2024-01-01", opponent: "Tottenham", score: "0-2", result: "L", isHome: false },
      { date: "2023-12-28", opponent: "Newcastle", score: "4-1", result: "W", isHome: true },
    ],
    awayTeamForm: [
      { date: "2024-01-16", opponent: "Chelsea", score: "1-0", result: "W", isHome: true },
      { date: "2024-01-11", opponent: "Man City", score: "2-2", result: "D", isHome: false },
      { date: "2024-01-06", opponent: "Liverpool", score: "1-3", result: "L", isHome: false },
      { date: "2024-01-02", opponent: "Arsenal", score: "2-1", result: "W", isHome: true },
      { date: "2023-12-29", opponent: "Brighton", score: "1-1", result: "D", isHome: false },
    ],
    ctas: {
      backHref: "/",
      shareHref: "/fixture/man-city-vs-liverpool",
      homeTeamHref: "/team/manchester-city",
      awayTeamHref: "/team/liverpool",
    },
    generatedAtIso: new Date().toISOString(),
  },
  "bayern-vs-dortmund": {
    fixture: {
      fixtureId: "bayern-vs-dortmund",
      slug: "bayern-vs-dortmund",
    },
    league: {
      leagueSeasonId: "bundesliga-2025-26",
      name: "Bundesliga",
      logoUrl: "https://media.api-sports.io/football/leagues/78.png",
      country: "Germany",
      countryLogoUrl: "https://flagcdn.com/w40/de.png",
      matchday: 18,
    },
    kickoff: {
      iso: "2099-02-15T17:30:00Z",
    },
    venue: {
      name: "Allianz Arena",
      city: "Munich",
      capacity: 75000,
    },
    homeTeam: {
      teamId: "bayern-munich",
      name: "Bayern Munich",
      shortName: "FCB",
      slug: "bayern-munich",
      logoUrl: "https://tmssl.akamaized.net/images/wappen/head/27.png",
      primaryColor: "#DC052D",
      avgGoalsScored: 3.1,
      avgGoalsConceded: 1.0,
      avgGoals: {
        overall: { scored: 3.1, conceded: 1.0 },
        home: { scored: 3.6, conceded: 0.6 },
        away: { scored: 2.6, conceded: 1.4 },
      },
      recentForm: ["W", "W", "W", "D", "W"],
      formByContext: {
        overall: ["W", "W", "W", "D", "W"],
        home: ["W", "W", "W", "W", "W"],
        away: ["W", "D", "W", "L", "W"],
      },
    },
    awayTeam: {
      teamId: "dortmund",
      name: "Borussia Dortmund",
      shortName: "BVB",
      slug: "dortmund",
      logoUrl: "https://tmssl.akamaized.net/images/wappen/head/16.png",
      primaryColor: "#FDE100",
      avgGoalsScored: 2.4,
      avgGoalsConceded: 1.5,
      avgGoals: {
        overall: { scored: 2.4, conceded: 1.5 },
        home: { scored: 2.8, conceded: 1.2 },
        away: { scored: 2.0, conceded: 1.8 },
      },
      recentForm: ["W", "D", "W", "L", "W"],
      formByContext: {
        overall: ["W", "D", "W", "L", "W"],
        home: ["W", "W", "D", "W", "W"],
        away: ["D", "W", "L", "W", "D"],
      },
    },
    homeStreaks: [
      { event: "Won at home", count: 8, scope: "home", polarity: "positive" },
      { event: "Scored 3+ goals", count: 5, scope: "overall", polarity: "positive" },
      { event: "Clean sheet at home", count: 4, scope: "home", polarity: "positive" },
    ],
    awayStreaks: [
      { event: "Unbeaten away", count: 6, scope: "away", polarity: "positive" },
      { event: "Scored 1+ goals", count: 12, scope: "overall", polarity: "positive" },
      { event: "BTTS", count: 7, total: 10, scope: "away", polarity: "neutral" },
    ],
    h2hStats: {
      totalMatches: 10,
      homeWins: 5,
      draws: 2,
      awayWins: 3,
      recentMatches: [
        { date: "2025-10-20", home: 3, away: 2, result: "H", isHomeTeamHome: true },
        { date: "2025-04-15", home: 1, away: 2, result: "A", isHomeTeamHome: false },
        { date: "2024-11-30", home: 2, away: 2, result: "D", isHomeTeamHome: true },
        { date: "2024-05-20", home: 0, away: 1, result: "A", isHomeTeamHome: false },
        { date: "2023-12-10", home: 3, away: 0, result: "H", isHomeTeamHome: true },
      ],
      streaks: [
        { event: "Both teams scored", count: 8, total: 10, teams: "both" },
        { event: "Over 2.5 goals", count: 7, total: 10, teams: "both" },
        { event: "Won at home", count: 3, total: 5, teams: "home" },
      ],
    },
    homeTeamForm: [
      { date: "2024-02-10", opponent: "Leipzig", score: "4-0", result: "W", isHome: true },
      { date: "2024-02-05", opponent: "Leverkusen", score: "2-1", result: "W", isHome: false },
      { date: "2024-01-30", opponent: "Frankfurt", score: "3-1", result: "W", isHome: true },
      { date: "2024-01-25", opponent: "Wolfsburg", score: "1-1", result: "D", isHome: false },
      { date: "2024-01-20", opponent: "Freiburg", score: "3-0", result: "W", isHome: true },
    ],
    awayTeamForm: [
      { date: "2024-02-11", opponent: "Gladbach", score: "2-0", result: "W", isHome: true },
      { date: "2024-02-06", opponent: "Stuttgart", score: "1-1", result: "D", isHome: false },
      { date: "2024-01-31", opponent: "Mainz", score: "3-2", result: "W", isHome: true },
      { date: "2024-01-26", opponent: "Union Berlin", score: "0-1", result: "L", isHome: false },
      { date: "2024-01-21", opponent: "Augsburg", score: "2-1", result: "W", isHome: true },
    ],
    ctas: {
      backHref: "/",
      shareHref: "/fixture/bayern-vs-dortmund",
      homeTeamHref: "/team/bayern-munich",
      awayTeamHref: "/team/dortmund",
    },
    generatedAtIso: new Date().toISOString(),
  },
}

/**
 * Default fixture for fallback (used by existing mock-fixture-data.ts consumers)
 */
export const DEFAULT_FIXTURE = MOCK_FIXTURES["man-city-vs-liverpool"]
