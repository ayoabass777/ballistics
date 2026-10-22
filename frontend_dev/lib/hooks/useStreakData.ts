import useSWR from "swr"
import { mockStreaks, type StreakData } from "@/lib/mock-streaks"

interface UseStreakDataReturn {
  streaks: StreakData[]
  loading: boolean
  error: string | null
  refetch: () => void
  isValidating: boolean
}

/**
 * Custom hook for fetching streak data with SWR
 * Handles both mock data and real API calls based on environment variable
 * Includes automatic caching, revalidation, and retry logic
 */
export function useStreakData(): UseStreakDataReturn {
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"

  // Always call useSWR — pass null key to disable fetching when using mock data
  const { data, error, isLoading, isValidating, mutate } = useSWR<StreakData[]>(
    useMockData ? null : "/api/streaks?scope=home",
    {
      fallbackData: mockStreaks.slice(0, 20),
      revalidateOnMount: true,
      refreshInterval: 300000,
      keepPreviousData: true,
      shouldRetryOnError: true,
    },
  )

  if (useMockData) {
    return {
      streaks: mockStreaks.slice(0, 20),
      loading: false,
      error: null,
      refetch: () => {},
      isValidating: false,
    }
  }

  return {
    streaks: data || mockStreaks.slice(0, 20),
    loading: isLoading,
    error: error?.message || null,
    refetch: () => mutate(),
    isValidating,
  }
}
