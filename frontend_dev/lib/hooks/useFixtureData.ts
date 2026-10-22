import useSWR from "swr"
import { mockFixture } from "@/lib/mock-fixture-data"
import type { FixturePageViewModel } from "@/types/fixture-page"

/**
 * Alias for backwards compatibility with existing components
 * Components can use either FixtureData or FixturePageViewModel
 */
export type FixtureData = FixturePageViewModel
export type FixtureDataStatus = "loading" | "ready" | "not_found" | "error"

export interface UseFixtureDataReturn {
  fixture: FixturePageViewModel | null
  loading: boolean
  status: FixtureDataStatus
  error: string | null
  refetch: () => void
  isValidating: boolean
}

/**
 * Custom hook for fetching fixture data with SWR
 * Handles both mock data and real API calls based on environment variable
 * Includes automatic caching, revalidation, and retry logic
 */
export function useFixtureData(fixtureId: string): UseFixtureDataReturn {
  // Check if we should use mock data via environment variable
  const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"

  // If using mock data, return it directly without SWR
  if (useMockData) {
    return {
      fixture: mockFixture,
      loading: false,
      status: "ready",
      error: null,
      refetch: () => {},
      isValidating: false,
    }
  }

  // Only use SWR when not using mock data
  const { data, error, isLoading, isValidating, mutate } = useSWR<FixturePageViewModel>(
    `/api/fixtures/${fixtureId}`,
    {
      revalidateOnMount: true,
      // Refresh data every 2 minutes (fixtures are more time-sensitive)
      refreshInterval: 120000,
      // Show stale data while revalidating
      keepPreviousData: true,
      // Retry on error
      shouldRetryOnError: (err) => (err as any)?.status !== 404,
      onError: (err) => {
        console.error(`Failed to fetch fixture ${fixtureId}:`, err)
      },
    },
  )

  let status: FixtureDataStatus = "loading"
  if (error) {
    status = (error as any)?.status === 404 ? "not_found" : "error"
  } else if (data) {
    status = "ready"
  } else if (!isLoading) {
    status = "error"
  }

  const errorMessage =
    status === "not_found"
      ? "Fixture analysis is not available for this match yet."
      : (error as any)?.info || error?.message || null

  return {
    fixture: status === "ready" ? data || null : null,
    loading: status === "loading",
    status,
    error: typeof errorMessage === "string" ? errorMessage : null,
    refetch: () => mutate(),
    isValidating,
  }
}
