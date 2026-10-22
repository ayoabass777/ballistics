import type { SWRConfiguration } from "swr"

/**
 * Global SWR configuration
 * Handles caching, revalidation, and error retry logic
 */
export const swrConfig: SWRConfiguration = {
  // Revalidate on focus to ensure fresh data
  revalidateOnFocus: true,

  // Revalidate on reconnect after network loss
  revalidateOnReconnect: true,

  // Don't revalidate on mount if data exists
  revalidateIfStale: false,

  // Dedupe requests within 2 seconds
  dedupingInterval: 2000,

  // Keep data fresh for 5 minutes
  focusThrottleInterval: 300000,

  // Retry on error with exponential backoff
  errorRetryCount: 3,
  errorRetryInterval: 5000,

  // Show stale data while revalidating
  keepPreviousData: true,

  // Default fetcher function
  fetcher: async (url: string) => {
    const response = await fetch(url)

    if (!response.ok) {
      const error = new Error("An error occurred while fetching the data.")
      // Attach extra info to the error object
      ;(error as any).info = await response.text()
      ;(error as any).status = response.status
      throw error
    }

    return response.json()
  },
}
