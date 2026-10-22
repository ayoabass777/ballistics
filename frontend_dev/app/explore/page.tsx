"use client"

import { useState, useEffect, useCallback, useRef, Suspense } from "react"
import { ArrowUpCircle, ArrowLeft, Search, Filter, TrendingUp, Zap, CalendarDays } from "lucide-react"
import { StreakGrid } from "@/components/StreakGrid"
import { LoadingSkeleton } from "@/components/LoadingSkeleton"
import { FloatingLeagueFilter } from "@/components/explore/FloatingLeagueFilter"
import { LeagueChips } from "@/components/ui/LeagueChips"
import { ActiveFilterPills } from "@/components/explore/ActiveFilterPills"
import { EmptyState } from "@/components/ui/EmptyState"
import { useRouter, useSearchParams, usePathname } from "next/navigation"
import Link from "next/link"
import type { StreakType } from "@/types/explore-page"
import { type StreakData } from "@/lib/mock-streaks"

interface ExploreStreakRow extends StreakData {
  resolved_streak_type: StreakType | null
}

interface ExplorePagination {
  cursor?: string | null
  next_cursor?: string | null
  nextCursor?: string | null
  limit?: number
  total?: number
  hasMore?: boolean
}

interface ExploreLeagueOption {
  id: string
  league_name: string
  country_name?: string | null
  league_logo_url?: string | null
  country_logo_url?: string | null
  label: string
  count?: number
  goal_streaks_count?: number
  result_streaks_count?: number
}

interface ExploreFilters {
  leagues?: string[]
  types?: string[]
  league_options?: ExploreLeagueOption[]
  type_counts?: {
    all?: number
    goal_streaks?: number
    result_streaks?: number
  }
}

const EXPLORE_PAGE_LIMIT = 50
const EXPLORE_RESTORE_LIMIT_MAX = 1000
const RESTORE_STATE_MAX_AGE_MS = 10 * 60 * 1000

const SCROLL_POSITION_KEY = "explore_scroll_position"
const RESTORE_SCROLL_TOLERANCE_PX = 120
const RESTORE_LOAD_ATTEMPT_LIMIT = 12

type ExploreTypeFilter = "all" | "goal_streaks" | "result_streaks"
type ExploreSortOption = "default" | "match_date_asc"

interface ExploreRestoreState {
  scrollY: number
  loadedCount?: number
  url?: string
  sort?: ExploreSortOption
  streaks?: ExploreStreakRow[]
  nextCursor?: string | null
  hasMore?: boolean
  totalStreaks?: number | null
  filters?: ExploreFilters | null
  leagueOptions?: ExploreLeagueOption[]
  savedAt?: number
}

function normalizeTypeFilterValue(value: string | null | undefined): ExploreTypeFilter {
  const normalized = (value || "").trim().toLowerCase()
  if (normalized === "goal_streaks" || normalized === "goal" || normalized === "goals") return "goal_streaks"
  if (normalized === "result_streaks" || normalized === "result" || normalized === "results") return "result_streaks"
  return "all"
}

function normalizeSortValue(value: string | null | undefined): ExploreSortOption {
  return value === "match_date_asc" ? "match_date_asc" : "default"
}

function normalizeRowStreakType(value: unknown): StreakType | null {
  const normalized = typeof value === "string" ? value.trim().toLowerCase().replace(/[\s-]+/g, "_") : ""
  if (normalized === "goal_streaks" || normalized === "goal" || normalized === "goals") return "goal_streaks"
  if (normalized === "result_streaks" || normalized === "result" || normalized === "results") return "result_streaks"
  return null
}

const RESULT_METRIC_KEYS = new Set(["win", "draw", "loss", "unbeaten", "winless", "halftime_win"])

function resolveStreakType(streak: Partial<StreakData> & { streakType?: unknown }): StreakType | null {
  const normalizedType =
    normalizeRowStreakType((streak as any).streak_type) ??
    normalizeRowStreakType((streak as any).streakType)
  if (normalizedType) return normalizedType

  const metricKey = String((streak as any).metric_key || (streak as any).metricKey || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_")
  if (!metricKey) return null
  return RESULT_METRIC_KEYS.has(metricKey) ? "result_streaks" : "goal_streaks"
}

function normalizeComparable(value: string | null | undefined): string {
  return String(value || "")
    .trim()
    .toLowerCase()
}

function toSlug(value: string | null | undefined): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
  return normalized || "item"
}

function toLeagueOptionId(leagueName: string, countryName?: string | null): string {
  return `${toSlug(countryName || "unknown")}-${toSlug(leagueName)}`
}

function parseExploreRestoreState(value: string | null, currentUrl?: string): ExploreRestoreState | null {
  if (!value) return null

  const numericPosition = Number.parseInt(value, 10)
  if (Number.isFinite(numericPosition)) {
    return { scrollY: Math.max(0, numericPosition) }
  }

  try {
    const parsed = JSON.parse(value) as Partial<ExploreRestoreState>
    const scrollY = Number(parsed.scrollY)
    if (!Number.isFinite(scrollY)) return null

    if (parsed.url && currentUrl && parsed.url !== currentUrl) return null

    const savedAt = Number(parsed.savedAt)
    if (Number.isFinite(savedAt) && Date.now() - savedAt > RESTORE_STATE_MAX_AGE_MS) {
      return null
    }

    const streaks = Array.isArray(parsed.streaks)
      ? parsed.streaks.filter((item): item is ExploreStreakRow => Boolean(item && typeof item === "object"))
      : undefined
    const loadedCount = Number(parsed.loadedCount)
    const totalStreaks = Number(parsed.totalStreaks)

    return {
      scrollY: Math.max(0, scrollY),
      loadedCount: Number.isFinite(loadedCount) ? Math.max(0, loadedCount) : streaks?.length,
      url: parsed.url,
      sort: normalizeSortValue(parsed.sort),
      streaks,
      nextCursor: typeof parsed.nextCursor === "string" ? parsed.nextCursor : null,
      hasMore: Boolean(parsed.hasMore),
      totalStreaks: Number.isFinite(totalStreaks) ? totalStreaks : null,
      filters: parsed.filters || null,
      leagueOptions: Array.isArray(parsed.leagueOptions) ? parsed.leagueOptions : undefined,
      savedAt: Number.isFinite(savedAt) ? savedAt : undefined,
    }
  } catch {
    return null
  }
}

const ALL_LEAGUES_OPTION: ExploreLeagueOption = {
  id: "all",
  league_name: "",
  country_name: null,
  label: "All Leagues",
}

function ExploreStreaksContent() {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()

  // Initialize state from URL params
  const initialType = normalizeTypeFilterValue(searchParams.get("type"))
  const initialLeagueId = searchParams.get("league") || "all"
  const initialSearch = searchParams.get("q") || ""
  const initialSort = normalizeSortValue(searchParams.get("sort"))

  const [selectedType, setSelectedType] = useState<ExploreTypeFilter>(initialType)
  const [streaks, setStreaks] = useState<ExploreStreakRow[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [showScrollToTop, setShowScrollToTop] = useState(false)
  const [searchQuery, setSearchQuery] = useState(initialSearch)
  const [selectedSort, setSelectedSort] = useState<ExploreSortOption>(initialSort)
  const [selectedLeagueId, setSelectedLeagueId] = useState(initialLeagueId)
  const [leagueOptions, setLeagueOptions] = useState<ExploreLeagueOption[]>([ALL_LEAGUES_OPTION])
  const [exploreFilters, setExploreFilters] = useState<ExploreFilters | null>(null)
  const [filtersHydrated, setFiltersHydrated] = useState(false)
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [hasMore, setHasMore] = useState(false)
  const [totalStreaks, setTotalStreaks] = useState<number | null>(null)
  const loadMoreRef = useRef<HTMLDivElement | null>(null)
  const inFlightCursorRef = useRef<string | null>(null)
  const pendingRestoreRef = useRef<ExploreRestoreState | null>(null)
  const restoreLoadAttemptsRef = useRef(0)
  const selectedLeague =
    leagueOptions.find((option) => option.id === selectedLeagueId) || ALL_LEAGUES_OPTION

  // Update URL when filters change
  const updateURL = useCallback(
    (type: string, league: string, search: string, sort: ExploreSortOption) => {
      const params = new URLSearchParams()
      if (type !== "all") params.set("type", type)
      if (league !== "all") params.set("league", league)
      if (search) params.set("q", search)
      if (sort !== "default") params.set("sort", sort)
      const queryString = params.toString()
      const newUrl = queryString ? `${pathname}?${queryString}` : pathname
      router.replace(newUrl, { scroll: false })
    },
    [pathname, router]
  )

  // Save restore snapshot before navigation.
  const saveScrollPosition = useCallback(() => {
    const rowsToRestore = streaks.slice(0, EXPLORE_RESTORE_LIMIT_MAX)
    const restoreState: ExploreRestoreState = {
      scrollY: window.scrollY,
      loadedCount: rowsToRestore.length,
      url: window.location.pathname + window.location.search,
      sort: selectedSort,
      streaks: rowsToRestore,
      nextCursor,
      hasMore,
      totalStreaks,
      filters: exploreFilters,
      leagueOptions,
      savedAt: Date.now(),
    }

    try {
      sessionStorage.setItem(SCROLL_POSITION_KEY, JSON.stringify(restoreState))
    } catch {
      sessionStorage.setItem(
        SCROLL_POSITION_KEY,
        JSON.stringify({
          scrollY: restoreState.scrollY,
          loadedCount: restoreState.loadedCount,
          url: restoreState.url,
          sort: restoreState.sort,
          savedAt: restoreState.savedAt,
        } satisfies ExploreRestoreState),
      )
    }
  }, [exploreFilters, hasMore, leagueOptions, nextCursor, selectedSort, streaks, totalStreaks])

  useEffect(() => {
    const restoreState = parseExploreRestoreState(
      sessionStorage.getItem(SCROLL_POSITION_KEY),
      window.location.pathname + window.location.search,
    )
    pendingRestoreRef.current = restoreState

    if (restoreState?.streaks?.length) {
      setStreaks(restoreState.streaks)
      setNextCursor(restoreState.nextCursor ?? null)
      setHasMore(Boolean(restoreState.hasMore && restoreState.nextCursor))
      setTotalStreaks(typeof restoreState.totalStreaks === "number" ? restoreState.totalStreaks : restoreState.streaks.length)
      setExploreFilters(restoreState.filters || null)
      setLeagueOptions(
        Array.isArray(restoreState.leagueOptions) && restoreState.leagueOptions.length > 0
          ? restoreState.leagueOptions
          : [ALL_LEAGUES_OPTION],
      )
      setFiltersHydrated(true)
      setLoading(false)
    }
  }, [])

  // Keep UI state synced with URL params (important for returnTo/back navigation).
  useEffect(() => {
    const urlType = normalizeTypeFilterValue(searchParams.get("type"))
    const urlLeague = searchParams.get("league") || "all"
    const urlSearch = searchParams.get("q") || ""
    const urlSort = normalizeSortValue(searchParams.get("sort"))

    setSelectedType(urlType)
    setSelectedLeagueId(urlLeague)
    setSearchQuery(urlSearch)
    setSelectedSort(urlSort)
  }, [searchParams])

  // Handle type change with URL update
  const handleTypeChange = useCallback(
    (type: ExploreTypeFilter) => {
      const normalizedType = normalizeTypeFilterValue(type)
      setSelectedType(normalizedType)
      updateURL(normalizedType, selectedLeagueId, searchQuery, selectedSort)
    },
    [selectedLeagueId, selectedSort, searchQuery, updateURL]
  )

  // Handle league change with URL update
  const handleLeagueChange = useCallback(
    (leagueId: string) => {
      setSelectedLeagueId(leagueId)
      updateURL(selectedType, leagueId, searchQuery, selectedSort)
    },
    [selectedType, selectedSort, searchQuery, updateURL]
  )

  // Handle search change with URL update (debounced)
  const handleSearchChange = useCallback(
    (query: string) => {
      setSearchQuery(query)
      updateURL(selectedType, selectedLeagueId, query, selectedSort)
    },
    [selectedType, selectedLeagueId, selectedSort, updateURL]
  )

  const handleSortChange = useCallback(
    (sort: ExploreSortOption) => {
      const normalizedSort = normalizeSortValue(sort)
      setSelectedSort(normalizedSort)
      updateURL(selectedType, selectedLeagueId, searchQuery, normalizedSort)
    },
    [selectedType, selectedLeagueId, searchQuery, updateURL],
  )

  // Handle league chip selection
  const handleLeagueChipChange = useCallback(
    (leagueId: string) => {
      handleLeagueChange(leagueId)
    },
    [handleLeagueChange]
  )

  // Clear all filters
  const handleClearFilters = useCallback(() => {
    setSelectedType("all")
    setSelectedLeagueId("all")
    setSearchQuery("")
    setSelectedSort("default")
    updateURL("all", "all", "", "default")
  }, [updateURL])

  const activeFilters = [
    ...(selectedType !== "all"
      ? [
          {
            key: "type",
            label: selectedType === "result_streaks" ? "Results" : "Goals",
            onRemove: () => handleTypeChange("all"),
          },
        ]
      : []),
    ...(selectedLeagueId !== "all"
      ? [
          {
            key: "league",
            label: selectedLeague.label,
            onRemove: () => handleLeagueChange("all"),
          },
        ]
      : []),
    ...(searchQuery
      ? [
          {
            key: "search",
            label: `"${searchQuery}"`,
            onRemove: () => handleSearchChange(""),
          },
        ]
      : []),
    ...(selectedSort !== "default"
      ? [
          {
            key: "sort",
            label: "Soonest first",
            onRemove: () => handleSortChange("default"),
          },
        ]
      : []),
  ]

  const normalizeRows = useCallback((rows: unknown[]): ExploreStreakRow[] => {
    return rows.map((row) => {
      const streak = row as StreakData
      return {
        ...streak,
        resolved_streak_type: resolveStreakType(streak as any),
      }
    })
  }, [])

  const normalizeLeagueOptions = useCallback(
    (filters: ExploreFilters | null | undefined, fallbackRows: ExploreStreakRow[]): ExploreLeagueOption[] => {
      const normalizedFromFilters = Array.isArray(filters?.league_options)
        ? filters.league_options
            .map((option) => {
              const leagueName = String(option?.league_name || "").trim()
              if (!leagueName) return null
              const countryName = String(option?.country_name || "").trim() || undefined
              const leagueLogoUrl = String((option as any)?.league_logo_url || (option as any)?.logo_url || "").trim() || undefined
              const countryLogoUrl = String((option as any)?.country_logo_url || "").trim() || undefined
              return {
                id: String(option?.id || "").trim() || toLeagueOptionId(leagueName, countryName),
                league_name: leagueName,
                country_name: countryName,
                league_logo_url: leagueLogoUrl,
                country_logo_url: countryLogoUrl,
                label: String(option?.label || "").trim() || (countryName ? `${leagueName} (${countryName})` : leagueName),
                count: typeof option?.count === "number" ? option.count : undefined,
                goal_streaks_count:
                  typeof (option as any)?.goal_streaks_count === "number"
                    ? (option as any).goal_streaks_count
                    : undefined,
                result_streaks_count:
                  typeof (option as any)?.result_streaks_count === "number"
                    ? (option as any).result_streaks_count
                    : undefined,
              } as ExploreLeagueOption
            })
            .filter((item): item is ExploreLeagueOption => item !== null)
        : []

      if (normalizedFromFilters.length > 0) {
        return [ALL_LEAGUES_OPTION, ...normalizedFromFilters]
      }

      const deduped = new Map<string, ExploreLeagueOption>()
      for (const row of fallbackRows) {
        const leagueName = String(row.league_name || "").trim()
        if (!leagueName) continue
        const countryName = String(row.country_name || "").trim() || undefined
        const leagueLogoUrl = String(row.league_logo_url || "").trim() || undefined
        const countryLogoUrl = String(row.country_logo_url || "").trim() || undefined
        const id = toLeagueOptionId(leagueName, countryName)
        const key = `${leagueName}::${countryName || ""}`
        const existing = deduped.get(key)
        if (!existing) {
          deduped.set(key, {
            id,
            league_name: leagueName,
            country_name: countryName,
            league_logo_url: leagueLogoUrl,
            country_logo_url: countryLogoUrl,
            label: countryName ? `${leagueName} (${countryName})` : leagueName,
            count: 1,
            goal_streaks_count: row.resolved_streak_type === "goal_streaks" ? 1 : 0,
            result_streaks_count: row.resolved_streak_type === "result_streaks" ? 1 : 0,
          })
        } else {
          existing.count = (existing.count || 0) + 1
          if (row.resolved_streak_type === "goal_streaks") {
            existing.goal_streaks_count = (existing.goal_streaks_count || 0) + 1
          }
          if (row.resolved_streak_type === "result_streaks") {
            existing.result_streaks_count = (existing.result_streaks_count || 0) + 1
          }
          if (!existing.league_logo_url && leagueLogoUrl) existing.league_logo_url = leagueLogoUrl
          if (!existing.country_logo_url && countryLogoUrl) existing.country_logo_url = countryLogoUrl
        }
      }

      const options = Array.from(deduped.values()).sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" }))
      return [ALL_LEAGUES_OPTION, ...options]
    },
    [],
  )

  const fetchExplorePage = useCallback(
    async (cursorToLoad?: string | null, options?: { restoreLimit?: number; silent?: boolean }) => {
      const isAppending = Boolean(cursorToLoad)
      const isSilentRefresh = Boolean(options?.silent && !isAppending)
      try {
        if (isAppending) {
          setLoadingMore(true)
        } else if (!isSilentRefresh) {
          setLoading(true)
          setError(null)
        }

        const params = new URLSearchParams({
          scope: "explore",
          limit: String(EXPLORE_PAGE_LIMIT),
        })
        if (cursorToLoad) params.set("cursor", cursorToLoad)
        if (!cursorToLoad && options?.restoreLimit && options.restoreLimit > EXPLORE_PAGE_LIMIT) {
          params.set("restoreLimit", String(Math.min(options.restoreLimit, EXPLORE_RESTORE_LIMIT_MAX)))
        }
        if (selectedSort !== "default") params.set("sort", selectedSort)

        const response = await fetch(`/api/streaks?${params.toString()}`)
        if (!response.ok) {
          const errorText = await response.text()
          throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`)
        }

        const payload = await response.json()
        const dataRows = Array.isArray(payload)
          ? payload
          : Array.isArray(payload?.data)
            ? payload.data
            : Array.isArray(payload?.data?.streaks)
              ? payload.data.streaks
              : []
        const normalizedRows = normalizeRows(dataRows)

        const pagination: ExplorePagination = Array.isArray(payload) ? {} : payload?.pagination || {}
        const filters: ExploreFilters | undefined = Array.isArray(payload)
          ? undefined
          : payload?.filters || payload?.data?.filters
        const next = pagination.next_cursor ?? pagination.nextCursor ?? null
        const more = Boolean(pagination.hasMore && next)
        const totalFromPagination =
          typeof pagination.total === "number" && Number.isFinite(pagination.total) ? pagination.total : null
        const nextLeagueOptions = normalizeLeagueOptions(filters, normalizedRows)

        if (isAppending) {
          setStreaks((prev) => {
            const seen = new Set(prev.map((item) => item.streak_id))
            const dedupedAppend = normalizedRows.filter((item) => !seen.has(item.streak_id))
            return [...prev, ...dedupedAppend]
          })
        } else {
          setStreaks(normalizedRows)
        }

        setNextCursor(next)
        setHasMore(more)
        setTotalStreaks((prev) => totalFromPagination ?? prev)
        setExploreFilters(filters || null)
        setLeagueOptions(nextLeagueOptions)
        setFiltersHydrated(true)
      } catch (e: any) {
        if (!isSilentRefresh) {
          setError(e.message || "Failed to fetch streaks")
        }
        console.error("Failed to fetch streaks:", e)
      } finally {
        setLoading(false)
        setLoadingMore(false)
      }
    },
    [normalizeRows, normalizeLeagueOptions, selectedSort]
  )

  const loadMore = useCallback(() => {
    if (!nextCursor || !hasMore || loading || loadingMore) return
    if (inFlightCursorRef.current === nextCursor) return
    inFlightCursorRef.current = nextCursor
    fetchExplorePage(nextCursor).finally(() => {
      inFlightCursorRef.current = null
    })
  }, [nextCursor, hasMore, loading, loadingMore, fetchExplorePage])

  useEffect(() => {
    const restoreState = pendingRestoreRef.current
    const restoreLimit = restoreState?.loadedCount
      ? Math.min(Math.max(restoreState.loadedCount, EXPLORE_PAGE_LIMIT), EXPLORE_RESTORE_LIMIT_MAX)
      : undefined

    fetchExplorePage(null, {
      restoreLimit,
      silent: Boolean(restoreState?.streaks?.length),
    })
  }, [fetchExplorePage])

  useEffect(() => {
    if (!filtersHydrated) return
    if (selectedLeagueId === "all") return
    if (leagueOptions.some((option) => option.id === selectedLeagueId)) return
    setSelectedLeagueId("all")
    updateURL(selectedType, "all", searchQuery, selectedSort)
  }, [filtersHydrated, selectedLeagueId, leagueOptions, selectedType, selectedSort, searchQuery, updateURL])

  useEffect(() => {
    if (!hasMore || loading || loadingMore) return
    const node = loadMoreRef.current
    if (!node) return

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          loadMore()
        }
      },
      { rootMargin: "400px 0px" },
    )

    observer.observe(node)
    return () => observer.disconnect()
  }, [hasMore, loading, loadingMore, loadMore])


  const matchesSelectedLeague = useCallback(
    (streak: ExploreStreakRow) => {
      if (selectedLeagueId === "all") return true
      const sameLeague = normalizeComparable(streak.league_name) === normalizeComparable(selectedLeague.league_name)
      if (!sameLeague) return false
      if (!selectedLeague.country_name) return true
      return normalizeComparable(streak.country_name) === normalizeComparable(selectedLeague.country_name)
    },
    [selectedLeagueId, selectedLeague.league_name, selectedLeague.country_name],
  )

  const fallbackAllCount =
    selectedLeagueId === "all" ? (totalStreaks ?? streaks.length) : streaks.filter((streak) => matchesSelectedLeague(streak)).length
  const fallbackGoalCount = streaks.filter(
    (streak) => matchesSelectedLeague(streak) && streak.resolved_streak_type === "goal_streaks",
  ).length
  const fallbackResultCount = streaks.filter(
    (streak) => matchesSelectedLeague(streak) && streak.resolved_streak_type === "result_streaks",
  ).length

  const selectedLeagueCounts = (() => {
    if (selectedLeagueId === "all") {
      const raw = exploreFilters?.type_counts
      return {
        all: typeof raw?.all === "number" ? raw.all : fallbackAllCount,
        goal_streaks: typeof raw?.goal_streaks === "number" ? raw.goal_streaks : fallbackGoalCount,
        result_streaks: typeof raw?.result_streaks === "number" ? raw.result_streaks : fallbackResultCount,
      }
    }

    const option = leagueOptions.find((league) => league.id === selectedLeagueId)
    if (!option) {
      return {
        all: fallbackAllCount,
        goal_streaks: fallbackGoalCount,
        result_streaks: fallbackResultCount,
      }
    }

    return {
      all: typeof option.count === "number" ? option.count : fallbackAllCount,
      goal_streaks: typeof option.goal_streaks_count === "number" ? option.goal_streaks_count : fallbackGoalCount,
      result_streaks:
        typeof option.result_streaks_count === "number" ? option.result_streaks_count : fallbackResultCount,
    }
  })()

  const filteredData = streaks
    .filter((streak) => {
      if (selectedType === "all") return true
      return streak.resolved_streak_type === selectedType
    })
    .filter((streak) => matchesSelectedLeague(streak))
    .filter((streak) => {
      if (!searchQuery) return true
      const query = searchQuery.toLowerCase()
      return (
        streak.team_name.toLowerCase().includes(query) ||
        streak.league_name.toLowerCase().includes(query) ||
        streak.next_opponent?.toLowerCase().includes(query)
      )
    })

  useEffect(() => {
    const restoreState = pendingRestoreRef.current
    if (!restoreState || loading || loadingMore) return

    const documentElement = document.documentElement
    const maxScrollY = Math.max(0, documentElement.scrollHeight - window.innerHeight)
    const canReachSavedPosition = maxScrollY >= restoreState.scrollY - RESTORE_SCROLL_TOLERANCE_PX

    if (!canReachSavedPosition && hasMore && nextCursor && restoreLoadAttemptsRef.current < RESTORE_LOAD_ATTEMPT_LIMIT) {
      restoreLoadAttemptsRef.current += 1
      loadMore()
      return
    }

    requestAnimationFrame(() => {
      window.scrollTo(0, Math.min(restoreState.scrollY, maxScrollY))
      pendingRestoreRef.current = null
      restoreLoadAttemptsRef.current = 0
      sessionStorage.removeItem(SCROLL_POSITION_KEY)
    })
  }, [filteredData.length, hasMore, loadMore, loading, loadingMore, nextCursor])

  const handleScroll = () => {
    if (window.scrollY > 300) {
      setShowScrollToTop(true)
    } else {
      setShowScrollToTop(false)
    }
  }

  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: "smooth" })
  }

  // Build current URL with params for returnTo
  const buildReturnUrl = useCallback(() => {
    const params = new URLSearchParams()
    if (selectedType !== "all") params.set("type", selectedType)
    if (selectedLeagueId !== "all") params.set("league", selectedLeagueId)
    if (searchQuery) params.set("q", searchQuery)
    if (selectedSort !== "default") params.set("sort", selectedSort)
    const queryString = params.toString()
    return queryString ? `/explore?${queryString}` : "/explore"
  }, [selectedType, selectedLeagueId, selectedSort, searchQuery])

  const handleViewTeamStats = useCallback(
    (teamName: string, fixtureId?: string) => {
      // Save scroll position before navigating
      saveScrollPosition()
      const returnUrl = encodeURIComponent(buildReturnUrl())

      if (fixtureId) {
        router.push(`/fixture/${fixtureId}?returnTo=${returnUrl}`)
      } else {
        const teamSlug = teamName.toLowerCase().replace(/\s+/g, "-")
        router.push(`/team/${encodeURIComponent(teamSlug)}?returnTo=${returnUrl}`)
      }
    },
    [router, saveScrollPosition, buildReturnUrl]
  )

  useEffect(() => {
    window.addEventListener("scroll", handleScroll)
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  useEffect(() => {
    const persistCurrentPosition = () => {
      if (window.location.pathname !== "/explore") return
      if (loading || streaks.length === 0) return
      saveScrollPosition()
    }

    const handleVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        persistCurrentPosition()
      }
    }

    window.addEventListener("pagehide", persistCurrentPosition)
    document.addEventListener("visibilitychange", handleVisibilityChange)

    return () => {
      window.removeEventListener("pagehide", persistCurrentPosition)
      document.removeEventListener("visibilitychange", handleVisibilityChange)
    }
  }, [loading, saveScrollPosition, streaks.length])

  return (
    <div className="min-h-screen p-4 sm:p-6 font-sans text-foreground">
      <FloatingLeagueFilter
        leagues={leagueOptions}
        selectedLeagueId={selectedLeagueId}
        onLeagueChange={handleLeagueChange}
        selectedCategory={selectedType}
        onCategoryChange={handleTypeChange}
      />

      <div className="max-w-7xl mx-auto">
        {/* Hero Section with Back Button */}
        <div className="pt-20 pb-8 sm:pt-24 sm:pb-12">
          <Link
            href="/"
            className="inline-flex items-center gap-2 mb-6 px-4 py-2.5 text-sm font-medium text-muted-foreground hover:text-foreground rounded-xl bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-primary/30 transition-all duration-200"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Home
          </Link>

          <div className="mb-8">
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 mb-4">
              <TrendingUp className="h-3.5 w-3.5 text-primary" />
              <span className="text-xs font-semibold text-primary">All Active Streaks</span>
            </div>
            <h1 className="text-3xl sm:text-4xl md:text-5xl font-bold mb-3 text-foreground tracking-tight">
              Explore Streaks
            </h1>
            <p className="text-base sm:text-lg text-muted-foreground">
              Discover {totalStreaks ?? streaks.length} live football streaks across all leagues
            </p>
            {typeof totalStreaks === "number" && totalStreaks > streaks.length && (
              <p className="text-sm text-muted-foreground mt-1">
                Showing {streaks.length} loaded
              </p>
            )}
          </div>

          {/* Search and Filters */}
          <div className="glass-panel p-4 sm:p-6 space-y-4">
            {/* Search Bar */}
            <div className="relative">
              <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
              <input
                type="text"
                placeholder="Search teams, leagues, or opponents..."
                value={searchQuery}
                onChange={(e) => handleSearchChange(e.target.value)}
                className="w-full pl-12 pr-4 py-3 bg-secondary/60 dark:bg-secondary/40 border border-border/50 rounded-xl text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary/40 focus:ring-2 focus:ring-primary/20 focus:bg-secondary/80 dark:focus:bg-secondary/60 transition-all duration-200"
              />
            </div>

            {/* Quick League Chips */}
            <LeagueChips
              selected={selectedLeagueId}
              leagues={leagueOptions}
              onChange={handleLeagueChipChange}
            />

            {/* Filter Pills */}
            <div className="flex flex-wrap items-center gap-2">
              <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
                <Filter className="h-4 w-4" />
                <span className="hidden sm:inline">Filter by:</span>
              </div>

              <button
                onClick={() => handleTypeChange("all")}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                  selectedType === "all"
                    ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                    : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                }`}
              >
                All Streaks
                <span className="ml-2 text-xs opacity-75">({selectedLeagueCounts.all})</span>
              </button>

              <button
                onClick={() => handleTypeChange("result_streaks")}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                  selectedType === "result_streaks"
                    ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                    : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                }`}
              >
                Results
                <span className="ml-2 text-xs opacity-75">
                  ({selectedLeagueCounts.result_streaks})
                </span>
              </button>

              <button
                onClick={() => handleTypeChange("goal_streaks")}
                className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                  selectedType === "goal_streaks"
                    ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                    : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                }`}
              >
                Goals
                <span className="ml-2 text-xs opacity-75">
                  ({selectedLeagueCounts.goal_streaks})
                </span>
              </button>

              <div className="basis-full sm:basis-auto sm:ml-auto flex items-center gap-2 pt-1 sm:pt-0">
                <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
                  <CalendarDays className="h-4 w-4" />
                  <span>Sort:</span>
                </div>
                <button
                  onClick={() => handleSortChange("default")}
                  className={`px-3 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                    selectedSort === "default"
                      ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                      : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                  }`}
                >
                  Top streaks
                </button>
                <button
                  onClick={() => handleSortChange("match_date_asc")}
                  className={`px-3 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                    selectedSort === "match_date_asc"
                      ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                      : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                  }`}
                >
                  Soonest first
                </button>
              </div>

            </div>

            {/* Results Count */}
            {(searchQuery || selectedLeagueId !== "all" || selectedType !== "all") && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Zap className="h-3.5 w-3.5 text-primary" />
                Found {filteredData.length} {filteredData.length === 1 ? "streak" : "streaks"}
                {selectedLeagueId !== "all" && ` in ${selectedLeague.label}`}
              </div>
            )}

            {/* Active Filter Pills */}
            {activeFilters.length > 0 && (
              <ActiveFilterPills
                filters={activeFilters}
                onClearAll={handleClearFilters}
              />
            )}
          </div>
        </div>

        {/* Streak Grid */}
        {loading ? (
          <LoadingSkeleton />
        ) : filteredData.length === 0 ? (
          <EmptyState
            title="No streaks found"
            description={
              searchQuery
                ? `No results matching "${searchQuery}"`
                : "Try adjusting your filters to see more streaks"
            }
            suggestions={[
              { label: "Clear search", action: () => handleSearchChange("") },
              { label: "Show all leagues", action: () => handleLeagueChange("all") },
              { label: "Reset sort", action: () => handleSortChange("default") },
            ]}
            onClearFilters={handleClearFilters}
          />
        ) : (
          <StreakGrid streaks={filteredData} loading={loading} error={error} onViewStats={handleViewTeamStats} />
        )}

        {!loading && (
          <div ref={loadMoreRef} className="py-6 text-center text-sm text-muted-foreground">
            {loadingMore ? "Loading more streaks..." : hasMore ? "Scroll to load more" : "All streaks loaded"}
          </div>
        )}
      </div>

      {/* Scroll to Top Button */}
      {showScrollToTop && (
        <div className="fixed bottom-6 left-6 z-50 transition-all duration-300 animate-fade-in">
          <button
            onClick={scrollToTop}
            className="p-3 rounded-full bg-secondary/80 dark:bg-secondary/60 backdrop-blur-md border border-border/50 hover:border-primary/30 text-foreground hover:text-primary shadow-lg hover:shadow-xl transition-all duration-200 flex items-center justify-center"
            aria-label="Scroll to top"
          >
            <ArrowUpCircle className="h-6 w-6" />
          </button>
        </div>
      )}
    </div>
  )
}

// Wrap with Suspense for useSearchParams
export default function ExploreStreaks() {
  return (
    <Suspense fallback={<LoadingSkeleton />}>
      <ExploreStreaksContent />
    </Suspense>
  )
}
