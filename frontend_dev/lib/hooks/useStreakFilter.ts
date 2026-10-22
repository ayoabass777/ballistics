import { useMemo } from "react"
import type { StreakData } from "@/lib/mock-streaks"

interface LeagueFilter {
  id: string
  name: string
  logo: string | null
}

interface UseStreakFilterReturn {
  filteredStreaks: StreakData[]
}

/**
 * Custom hook for filtering streaks by league
 * Uses useMemo to cache filtered results
 */
export function useStreakFilter(
  streaks: StreakData[],
  selectedLeague: LeagueFilter,
): UseStreakFilterReturn {
  const filteredStreaks = useMemo(() => {
    if (selectedLeague.id === "all") {
      return streaks
    }
    return streaks.filter((streak) => streak.league_name === selectedLeague.name)
  }, [streaks, selectedLeague])

  return { filteredStreaks }
}
