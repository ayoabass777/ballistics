"use client"

import { useEffect, useMemo, useState } from "react"
import { Globe, ChevronDown, ChevronRight, SlidersHorizontal } from "lucide-react"
import type { StreakType } from "@/types/explore-page"

interface LeagueOption {
  id: string
  label: string
  league_name?: string
  country_name?: string | null
  league_logo_url?: string | null
  country_logo_url?: string | null
}

type CategoryType = "all" | StreakType

interface FloatingLeagueFilterProps {
  leagues: LeagueOption[]
  selectedLeagueId: string
  onLeagueChange: (leagueId: string) => void
  selectedCategory?: CategoryType
  onCategoryChange?: (category: CategoryType) => void
}

const categories = [
  { id: "all" as CategoryType, name: "All" },
  { id: "result_streaks" as CategoryType, name: "Results" },
  { id: "goal_streaks" as CategoryType, name: "Goals" },
]

export function FloatingLeagueFilter({
  leagues,
  selectedLeagueId,
  onLeagueChange,
  selectedCategory = "all",
  onCategoryChange,
}: FloatingLeagueFilterProps) {
  const [isSticky, setIsSticky] = useState(false)
  const [isLeagueOpen, setIsLeagueOpen] = useState(false)
  const [isCategoryOpen, setIsCategoryOpen] = useState(false)
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null)

  const selectedLeague =
    leagues.find((league) => league.id === selectedLeagueId) ||
    leagues.find((league) => league.id === "all") ||
    { id: "all", label: "All Leagues" }

  const allOption = leagues.find((league) => league.id === "all") || { id: "all", label: "All Leagues" }

  const groupedLeagues = useMemo(() => {
    const groups = new Map<string, LeagueOption[]>()
    for (const league of leagues) {
      if (league.id === "all") continue
      const country = String(league.country_name || "Other").trim() || "Other"
      const current = groups.get(country) || []
      current.push(league)
      groups.set(country, current)
    }
    return Array.from(groups.entries())
      .sort((a, b) => a[0].localeCompare(b[0], undefined, { sensitivity: "base" }))
      .map(([country, options]) => ({
        country,
        countryLogoUrl:
          options.find((item) => String(item.country_logo_url || "").trim())?.country_logo_url || undefined,
        leagues: options.sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: "base" })),
      }))
  }, [leagues])

  useEffect(() => {
    const handleScroll = () => {
      setIsSticky(window.scrollY > 400)
    }

    window.addEventListener("scroll", handleScroll)
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement
      if (!target.closest(".filter-dropdown")) {
        setIsLeagueOpen(false)
        setIsCategoryOpen(false)
        setExpandedCountry(null)
      }
    }
    document.addEventListener("click", handleClickOutside)
    return () => document.removeEventListener("click", handleClickOutside)
  }, [])

  return (
    <div
      className={`transition-all duration-300 z-40 ${
        isSticky
          ? "fixed top-4 left-1/2 -translate-x-1/2 w-[calc(100%-2rem)] max-w-4xl animate-fade-in"
          : "relative opacity-0 pointer-events-none"
      }`}
    >
      <div className="relative backdrop-blur-xl bg-background/85 dark:bg-background/80 border border-border/50 dark:border-border/40 rounded-2xl px-4 sm:px-6 py-3 shadow-lg shadow-black/5 dark:shadow-black/20">
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-primary/30 to-transparent" />

        <div className="flex items-center gap-3 sm:gap-4">
          {onCategoryChange && (
            <div className="relative flex-shrink-0 filter-dropdown">
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  setIsCategoryOpen(!isCategoryOpen)
                  setIsLeagueOpen(false)
                }}
                className={`flex items-center gap-2 px-3 sm:px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                  selectedCategory !== "all"
                    ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                    : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                }`}
              >
                <SlidersHorizontal className="h-4 w-4" />
                <span className="hidden sm:inline">
                  {categories.find((c) => c.id === selectedCategory)?.name || "All"}
                </span>
                <ChevronDown className={`h-3.5 w-3.5 transition-transform ${isCategoryOpen ? "rotate-180" : ""}`} />
              </button>

              {isCategoryOpen && (
                <div className="absolute top-full left-0 mt-2 w-48 rounded-xl bg-popover/98 backdrop-blur-xl border border-border/60 shadow-xl shadow-black/10 dark:shadow-black/30 overflow-hidden z-50">
                  {categories.map((category) => (
                    <button
                      key={category.id}
                      onClick={() => {
                        onCategoryChange(category.id)
                        setIsCategoryOpen(false)
                      }}
                      className={`w-full flex items-center px-4 py-3 text-sm text-left transition-all ${
                        selectedCategory === category.id
                          ? "bg-primary/10 dark:bg-primary/20 text-primary font-medium"
                          : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40"
                      }`}
                    >
                      {category.name}
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}

          <div className="h-6 w-px bg-border flex-shrink-0" />

          <div className="relative flex-1 min-w-0 filter-dropdown">
            <button
              onClick={(e) => {
                e.stopPropagation()
                setIsLeagueOpen(!isLeagueOpen)
                setIsCategoryOpen(false)
              }}
                className={`w-full flex items-center gap-2 px-3 sm:px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
                  selectedLeagueId !== "all"
                    ? "bg-primary text-primary-foreground shadow-lg shadow-primary/25"
                    : "bg-secondary/60 dark:bg-secondary/40 text-muted-foreground hover:text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50"
                }`}
              >
              {selectedLeagueId !== "all" && selectedLeague.league_logo_url ? (
                <span className="flex h-5 w-5 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
                  <img src={selectedLeague.league_logo_url || "/placeholder.svg"} alt="" className="h-4 w-4 object-contain" />
                </span>
              ) : (
                <Globe className="h-4 w-4 flex-shrink-0" />
              )}
              <span className="truncate flex-1 text-left">{selectedLeague.label}</span>
              <ChevronDown
                className={`h-3.5 w-3.5 flex-shrink-0 transition-transform ${isLeagueOpen ? "rotate-180" : ""}`}
              />
            </button>

            {isLeagueOpen && (
              <div className="absolute top-full left-0 right-0 mt-2 rounded-xl bg-popover/98 backdrop-blur-xl border border-border/60 shadow-xl shadow-black/10 dark:shadow-black/30 overflow-hidden z-50 max-h-96 overflow-y-auto">
                <button
                  onClick={() => {
                    onLeagueChange(allOption.id)
                    setIsLeagueOpen(false)
                    setExpandedCountry(null)
                  }}
                  className={`w-full flex items-center gap-3 px-4 py-3 text-sm transition-all border-b border-border/40 ${
                    selectedLeagueId === allOption.id
                      ? "bg-primary/10 dark:bg-primary/20 text-primary font-medium"
                      : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40"
                  }`}
                >
                  <div className="flex items-center justify-center w-6 h-6 rounded-lg bg-primary/10 dark:bg-primary/20 border border-primary/20">
                    <Globe className="h-4 w-4 text-primary" />
                  </div>
                  <span className="font-medium">{allOption.label}</span>
                </button>

                {groupedLeagues.map((group) => (
                  <div key={group.country} className="border-b border-border/40 last:border-0">
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        setExpandedCountry(expandedCountry === group.country ? null : group.country)
                      }}
                      className="w-full px-4 py-2.5 flex items-center justify-between gap-2 hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-150"
                    >
                      <div className="flex items-center gap-2">
                        {group.countryLogoUrl ? (
                          <img src={group.countryLogoUrl} alt="" className="h-4 w-4 rounded-sm object-cover" />
                        ) : (
                          <Globe className="h-4 w-4 text-muted-foreground" />
                        )}
                        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                          {group.country}
                        </span>
                      </div>
                      <ChevronRight
                        className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ${expandedCountry === group.country ? "rotate-90" : ""}`}
                      />
                    </button>

                    <div
                      className={`overflow-hidden transition-all duration-200 ${expandedCountry === group.country ? "max-h-96 opacity-100" : "max-h-0 opacity-0"}`}
                    >
                      {group.leagues.map((league) => (
                        <button
                          key={league.id}
                          onClick={(e) => {
                            e.stopPropagation()
                            onLeagueChange(league.id)
                            setIsLeagueOpen(false)
                            setExpandedCountry(null)
                          }}
                          className={`w-full flex items-center gap-3 px-4 pl-10 py-2.5 text-sm text-left transition-all duration-150 ${
                            selectedLeagueId === league.id
                              ? "bg-primary/10 dark:bg-primary/15 text-primary border-l-2 border-primary font-medium"
                              : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 hover:pl-11"
                          }`}
                        >
                          <span className="flex h-6 w-6 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
                            {league.league_logo_url ? (
                              <img src={league.league_logo_url} alt="" className="h-5 w-5 object-contain" />
                            ) : (
                              <Globe className="h-4 w-4 text-muted-foreground" />
                            )}
                          </span>
                          <span className="font-medium">{league.label}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
