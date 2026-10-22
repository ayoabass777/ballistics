export interface TeamSearchSuggestion {
  teamId: number
  name: string
  slug: string
  href: string
  logoUrl?: string | null
  leagueName?: string | null
  countryLogoUrl?: string | null
}

export interface TeamSearchResponse {
  suggestions: TeamSearchSuggestion[]
  meta: {
    source: string
    query: string
    limit: number
    generatedAtIso?: string
    fallback?: boolean
    error?: string
  }
}
