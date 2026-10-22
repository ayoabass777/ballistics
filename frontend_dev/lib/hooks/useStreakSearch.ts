import { useMemo, useState, useCallback, useEffect } from "react"
import type { StreakData } from "@/lib/mock-streaks"

interface UseStreakSearchReturn {
  searchQuery: string
  setSearchQuery: (query: string) => void
  debouncedQuery: string
  filteredStreaks: StreakData[]
}

/**
 * Custom hook for searching streaks with debouncing
 * Searches across team name, league name, and opponent
 */
export function useStreakSearch(
  streaks: StreakData[],
  debounceMs: number = 300,
): UseStreakSearchReturn {
  const [searchQuery, setSearchQuery] = useState("")
  const [debouncedQuery, setDebouncedQuery] = useState("")

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(searchQuery)
    }, debounceMs)

    return () => clearTimeout(timer)
  }, [searchQuery, debounceMs])

  const filteredStreaks = useMemo(() => {
    if (!debouncedQuery) return streaks

    const query = debouncedQuery.toLowerCase()
    return streaks.filter(
      (streak) =>
        streak.team_name.toLowerCase().includes(query) ||
        streak.league_name.toLowerCase().includes(query) ||
        streak.next_opponent?.toLowerCase().includes(query),
    )
  }, [streaks, debouncedQuery])

  return {
    searchQuery,
    setSearchQuery,
    debouncedQuery,
    filteredStreaks,
  }
}
