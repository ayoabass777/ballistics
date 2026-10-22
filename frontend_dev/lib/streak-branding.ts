import type { StreakData } from "@/lib/mock-streaks"

type BrandingFields = Pick<
  StreakData,
  "team_logo_url" | "league_logo_url" | "country_name" | "country_logo_url"
>

type BrandingInput = Pick<StreakData, "team_name" | "league_name"> & Partial<BrandingFields>

const LEAGUE_TO_COUNTRY: Record<string, string> = {
  "Premier League": "England",
  Championship: "England",
  Bundesliga: "Germany",
  "Serie A": "Italy",
  "La Liga": "Spain",
  "Ligue 1": "France",
}

const LEAGUE_TO_COUNTRY_LOGO: Record<string, string> = {
  "Premier League": "https://flagcdn.com/w40/gb-eng.png",
  Championship: "https://flagcdn.com/w40/gb-eng.png",
  Bundesliga: "https://flagcdn.com/w40/de.png",
  "Serie A": "https://flagcdn.com/w40/it.png",
  "La Liga": "https://flagcdn.com/w40/es.png",
  "Ligue 1": "https://flagcdn.com/w40/fr.png",
}

const LEAGUE_TO_LOGO: Record<string, string> = {
  "Premier League": "https://resources.premierleague.com/premierleague/badges/t3/PL_logo_badge.png",
  Championship: "https://tmssl.akamaized.net/images/logo/header/gb2.png",
  Bundesliga: "https://tmssl.akamaized.net/images/logo/header/l1.png",
  "Serie A": "https://tmssl.akamaized.net/images/logo/header/it1.png",
  "La Liga": "https://tmssl.akamaized.net/images/logo/header/es1.png",
  "Ligue 1": "https://tmssl.akamaized.net/images/logo/header/fr1.png",
}

const TEAM_TO_LOGO: Record<string, string> = {
  "Manchester City": "https://resources.premierleague.com/premierleague/badges/50/t43.png",
  Liverpool: "https://resources.premierleague.com/premierleague/badges/50/t14.png",
  Arsenal: "https://resources.premierleague.com/premierleague/badges/50/t3.png",
  "Aston Villa": "https://resources.premierleague.com/premierleague/badges/50/t7.png",
  Tottenham: "https://resources.premierleague.com/premierleague/badges/50/t6.png",
  Chelsea: "https://resources.premierleague.com/premierleague/badges/50/t8.png",
  "Newcastle United": "https://resources.premierleague.com/premierleague/badges/50/t4.png",
  Brighton: "https://resources.premierleague.com/premierleague/badges/50/t36.png",
  "West Ham": "https://resources.premierleague.com/premierleague/badges/50/t21.png",
  "Manchester United": "https://resources.premierleague.com/premierleague/badges/50/t1.png",
  Everton: "https://resources.premierleague.com/premierleague/badges/50/t11.png",
  Fulham: "https://resources.premierleague.com/premierleague/badges/50/t54.png",
  "Crystal Palace": "https://resources.premierleague.com/premierleague/badges/50/t31.png",
  Bournemouth: "https://resources.premierleague.com/premierleague/badges/50/t91.png",
  "Nottingham Forest": "https://resources.premierleague.com/premierleague/badges/50/t17.png",
  Wolves: "https://resources.premierleague.com/premierleague/badges/50/t39.png",
  Brentford: "https://resources.premierleague.com/premierleague/badges/50/t94.png",
  Southampton: "https://resources.premierleague.com/premierleague/badges/50/t20.png",
  "Ipswich Town": "https://resources.premierleague.com/premierleague/badges/50/t40.png",
  "Leicester City": "https://resources.premierleague.com/premierleague/badges/50/t13.png",
  "Bayern Munich": "https://tmssl.akamaized.net/images/wappen/head/27.png",
  "Borussia Dortmund": "https://tmssl.akamaized.net/images/wappen/head/16.png",
  "Bayer Leverkusen": "https://tmssl.akamaized.net/images/wappen/head/15.png",
  "RB Leipzig": "https://tmssl.akamaized.net/images/wappen/head/23826.png",
  "Inter Milan": "https://tmssl.akamaized.net/images/wappen/head/46.png",
  "AC Milan": "https://tmssl.akamaized.net/images/wappen/head/5.png",
  Napoli: "https://tmssl.akamaized.net/images/wappen/head/6195.png",
  Juventus: "https://tmssl.akamaized.net/images/wappen/head/506.png",
  "Real Madrid": "https://tmssl.akamaized.net/images/wappen/head/418.png",
  Barcelona: "https://tmssl.akamaized.net/images/wappen/head/131.png",
  "Atletico Madrid": "https://tmssl.akamaized.net/images/wappen/head/13.png",
  Sevilla: "https://tmssl.akamaized.net/images/wappen/head/368.png",
  PSG: "https://tmssl.akamaized.net/images/wappen/head/583.png",
  Monaco: "https://tmssl.akamaized.net/images/wappen/head/162.png",
  Lyon: "https://tmssl.akamaized.net/images/wappen/head/1041.png",
  Marseille: "https://tmssl.akamaized.net/images/wappen/head/244.png",
  "Leeds United": "https://tmssl.akamaized.net/images/wappen/head/399.png",
  "Luton Town": "https://tmssl.akamaized.net/images/wappen/head/1031.png",
  "Sheffield United": "https://tmssl.akamaized.net/images/wappen/head/350.png",
  Burnley: "https://tmssl.akamaized.net/images/wappen/head/1132.png",
}

function normalizeTeamName(value: string): string {
  return value
    .toLowerCase()
    .replace(/\./g, "")
    .replace(/['’]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(football club|soccer club|fc|cf|afc|cfc|sc)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
}

const TEAM_ALIAS_TO_CANONICAL: Record<string, string> = {
  "man city": "Manchester City",
  "manchester city": "Manchester City",
  "man utd": "Manchester United",
  "man united": "Manchester United",
  "manchester united": "Manchester United",
  "inter": "Inter Milan",
  "inter milan": "Inter Milan",
  "ac milan": "AC Milan",
  "atletico": "Atletico Madrid",
  "atletico madrid": "Atletico Madrid",
  "psg": "PSG",
}

const NORMALIZED_TEAM_TO_CANONICAL: Record<string, string> = Object.keys(TEAM_TO_LOGO).reduce(
  (acc, canonicalName) => {
    acc[normalizeTeamName(canonicalName)] = canonicalName
    return acc
  },
  {} as Record<string, string>
)

function teamLogoFallback(teamName: string | undefined): string | undefined {
  const raw = typeof teamName === "string" ? teamName.trim() : ""
  if (!raw) return undefined

  if (TEAM_TO_LOGO[raw]) return TEAM_TO_LOGO[raw]

  const normalized = normalizeTeamName(raw)
  const aliasCanonical = TEAM_ALIAS_TO_CANONICAL[normalized]
  if (aliasCanonical && TEAM_TO_LOGO[aliasCanonical]) return TEAM_TO_LOGO[aliasCanonical]

  const directCanonical = NORMALIZED_TEAM_TO_CANONICAL[normalized]
  if (directCanonical && TEAM_TO_LOGO[directCanonical]) return TEAM_TO_LOGO[directCanonical]

  return undefined
}

function pickString(primary: string | undefined, fallback?: string): string | undefined {
  const normalizedPrimary = typeof primary === "string" ? primary.trim() : ""
  return normalizedPrimary || fallback
}

export function enrichStreakBranding<T extends BrandingInput>(row: T): T {
  const leagueName = row.league_name
  const enriched: T = { ...row }

  const teamLogo = pickString(enriched.team_logo_url, teamLogoFallback(row.team_name))
  if (teamLogo) enriched.team_logo_url = teamLogo

  const leagueLogo = pickString(enriched.league_logo_url, LEAGUE_TO_LOGO[leagueName])
  if (leagueLogo) enriched.league_logo_url = leagueLogo

  const countryName = pickString(enriched.country_name, LEAGUE_TO_COUNTRY[leagueName])
  if (countryName) enriched.country_name = countryName

  const countryLogo = pickString(enriched.country_logo_url, LEAGUE_TO_COUNTRY_LOGO[leagueName])
  if (countryLogo) enriched.country_logo_url = countryLogo

  return enriched
}
