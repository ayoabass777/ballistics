import { useEffect, useState } from "react"
import useSWR from "swr"
import type { TeamSearchResponse, TeamSearchSuggestion } from "@/types/search"

interface UseTeamSearchAutocompleteReturn {
  suggestions: TeamSearchSuggestion[]
  loading: boolean
  error: string | null
}

const MIN_QUERY_LENGTH = 2
const DEBOUNCE_MS = 180
const LIMIT = 6

const fetcher = async (url: string): Promise<TeamSearchResponse> => {
  const response = await fetch(url, { cache: "no-store" })
  if (!response.ok) {
    throw new Error(`Team search request failed (${response.status})`)
  }
  return (await response.json()) as TeamSearchResponse
}

export function useTeamSearchAutocomplete(query: string): UseTeamSearchAutocompleteReturn {
  const [debouncedQuery, setDebouncedQuery] = useState(query.trim())

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(query.trim()), DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  const enabled = debouncedQuery.length >= MIN_QUERY_LENGTH
  const key = enabled ? `/api/search/teams?q=${encodeURIComponent(debouncedQuery)}&limit=${LIMIT}` : null

  const { data, error, isLoading } = useSWR<TeamSearchResponse>(key, fetcher, {
    keepPreviousData: true,
    revalidateOnFocus: false,
    shouldRetryOnError: true,
  })

  return {
    suggestions: data?.suggestions || [],
    loading: enabled ? isLoading : false,
    error: error?.message || data?.meta.error || null,
  }
}
