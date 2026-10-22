export interface HomeStatsData {
  activeStreaks: number
  leagues: number
  teamsTracked: number
}

export interface HomeStatsMeta {
  source: string
  fallback: boolean
  generatedAtIso?: string
  backendSource?: string
  error?: string
}

export interface HomeStatsResponse {
  data: HomeStatsData
  meta: HomeStatsMeta
}
