export interface NavLeagueOption {
  id: string
  name: string
  logo: string | null
  href: string
  leagueSeasonId?: number | null
}

export interface NavCountryGroup {
  country: string
  countryLogoUrl?: string | null
  countryFlag?: string | null
  leagues: NavLeagueOption[]
}

export interface HomeNavMeta {
  source: string
  fallback: boolean
  generatedAtIso?: string
  backendSource?: string
  deduped?: boolean
  fallbackSlugCount?: number
  error?: string
}

export interface HomeNavResponse {
  leagueGroups: NavCountryGroup[]
  meta: HomeNavMeta
}
