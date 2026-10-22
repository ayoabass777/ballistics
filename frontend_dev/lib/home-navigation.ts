import type { HomeNavResponse, NavCountryGroup } from "@/types/navigation"

export const STATIC_HOME_LEAGUE_GROUPS: NavCountryGroup[] = [
  {
    country: "England",
    countryFlag: "🏴",
    leagues: [
      {
        id: "premier-league",
        name: "Premier League",
        logo: "https://media.api-sports.io/football/leagues/39.png",
        href: "/league/premier-league",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "Spain",
    countryFlag: "🇪🇸",
    leagues: [
      {
        id: "la-liga",
        name: "La Liga",
        logo: "https://media.api-sports.io/football/leagues/140.png",
        href: "/league/la-liga",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "Germany",
    countryFlag: "🇩🇪",
    leagues: [
      {
        id: "bundesliga",
        name: "Bundesliga",
        logo: "https://media.api-sports.io/football/leagues/78.png",
        href: "/league/bundesliga",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "Italy",
    countryFlag: "🇮🇹",
    leagues: [
      {
        id: "serie-a",
        name: "Serie A",
        logo: "https://media.api-sports.io/football/leagues/135.png",
        href: "/league/serie-a",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "France",
    countryFlag: "🇫🇷",
    leagues: [
      {
        id: "ligue-1",
        name: "Ligue 1",
        logo: "https://media.api-sports.io/football/leagues/61.png",
        href: "/league/ligue-1",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "Norway",
    countryFlag: "🇳🇴",
    leagues: [
      {
        id: "1-division",
        name: "1. Division",
        logo: null,
        href: "/league/1-division",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "Sweden",
    countryFlag: "🇸🇪",
    leagues: [
      {
        id: "division-2-sodra-svealand",
        name: "Division 2 - Södra Svealand",
        logo: null,
        href: "/league/division-2-sodra-svealand",
        leagueSeasonId: null,
      },
    ],
  },
  {
    country: "China",
    countryFlag: "🇨🇳",
    leagues: [
      {
        id: "super-league",
        name: "Super League",
        logo: null,
        href: "/league/super-league",
        leagueSeasonId: null,
      },
      {
        id: "league-one",
        name: "League One",
        logo: null,
        href: "/league/league-one",
        leagueSeasonId: null,
      },
      {
        id: "league-two",
        name: "League Two",
        logo: null,
        href: "/league/league-two",
        leagueSeasonId: null,
      },
    ],
  },
]

export function buildFallbackHomeNavResponse(error?: string): HomeNavResponse {
  return {
    leagueGroups: STATIC_HOME_LEAGUE_GROUPS,
    meta: {
      source: "fallback-static",
      fallback: true,
      ...(error ? { error } : {}),
    },
  }
}
