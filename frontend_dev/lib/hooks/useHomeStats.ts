import useSWR from "swr"
import type { HomeStatsResponse } from "@/types/home-stats"

interface UseHomeStatsReturn {
  stats: HomeStatsResponse["data"]
  loading: boolean
  error: string | null
  refetch: () => void
  isValidating: boolean
}

const FALLBACK_STATS: HomeStatsResponse["data"] = {
  activeStreaks: 500,
  leagues: 10,
  teamsTracked: 250,
}

const fetcher = async (url: string): Promise<HomeStatsResponse> => {
  const response = await fetch(url, { cache: "no-store" })
  if (!response.ok) {
    throw new Error(`Home stats request failed (${response.status})`)
  }
  return (await response.json()) as HomeStatsResponse
}

export function useHomeStats(): UseHomeStatsReturn {
  const { data, error, isLoading, isValidating, mutate } = useSWR<HomeStatsResponse>(
    "/api/home/stats",
    fetcher,
    {
      fallbackData: {
        data: FALLBACK_STATS,
        meta: {
          source: "fallback-static",
          fallback: true,
        },
      },
      revalidateOnMount: true,
      refreshInterval: 300000,
      keepPreviousData: true,
      shouldRetryOnError: true,
    },
  )

  const fallbackError = data?.meta.fallback ? data.meta.error || null : null

  return {
    stats: data?.data || FALLBACK_STATS,
    loading: isLoading,
    error: error?.message || fallbackError,
    refetch: () => mutate(),
    isValidating,
  }
}
