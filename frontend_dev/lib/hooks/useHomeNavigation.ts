import useSWR from "swr"
import { buildFallbackHomeNavResponse, STATIC_HOME_LEAGUE_GROUPS } from "@/lib/home-navigation"
import type { HomeNavResponse } from "@/types/navigation"

interface UseHomeNavigationReturn {
  leagueGroups: HomeNavResponse["leagueGroups"]
  loading: boolean
  error: string | null
  refetch: () => void
  isValidating: boolean
}

const fetcher = async (url: string): Promise<HomeNavResponse> => {
  const response = await fetch(url, { cache: "no-store" })
  if (!response.ok) {
    throw new Error(`Navigation request failed (${response.status})`)
  }
  return (await response.json()) as HomeNavResponse
}

export function useHomeNavigation(): UseHomeNavigationReturn {
  const { data, error, isLoading, isValidating, mutate } = useSWR<HomeNavResponse>(
    "/api/navigation/leagues",
    fetcher,
    {
      fallbackData: buildFallbackHomeNavResponse(),
      revalidateOnMount: true,
      refreshInterval: 300000,
      keepPreviousData: true,
      shouldRetryOnError: true,
    },
  )

  const fallbackError = data?.meta.fallback ? data.meta.error || null : null

  return {
    leagueGroups: data?.leagueGroups || STATIC_HOME_LEAGUE_GROUPS,
    loading: isLoading,
    error: error?.message || fallbackError,
    refetch: () => mutate(),
    isValidating,
  }
}
