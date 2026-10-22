"use client"

import { useState, useEffect, useRef } from "react"
import { StreakMiniCard } from "./StreakMiniCard"
import { EmptyState } from "@/components/ui/EmptyState"
import type { StreakCardVM, TeamIdentity } from "@/types/team-page"

type CurrentStreaksProps = {
  currentStreaks: {
    primary: StreakCardVM[]
    secondary?: StreakCardVM[]
  }
  team: TeamIdentity
  showSecondary: boolean
}

export function CurrentStreaks({ currentStreaks, team, showSecondary }: CurrentStreaksProps) {
  const [contextFilter, setContextFilter] = useState<"all" | "home" | "away">("all")
  const [categoryFilter, setCategoryFilter] = useState<"all" | "result" | "goal">("all")
  const [isFilterSticky, setIsFilterSticky] = useState(false)
  const filterBarRef = useRef<HTMLDivElement>(null)
  const sectionRef = useRef<HTMLElement>(null)

  useEffect(() => {
    const handleScroll = () => {
      if (filterBarRef.current && sectionRef.current) {
        const filterBarTop = filterBarRef.current.getBoundingClientRect().top
        const sectionBottom = sectionRef.current.getBoundingClientRect().bottom
        // Sticky when filter bar reaches header (64px), but not past section end
        setIsFilterSticky(filterBarTop <= 64 && sectionBottom > 150)
      }
    }

    window.addEventListener("scroll", handleScroll, { passive: true })
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  const displayStreaks = showSecondary
    ? [...currentStreaks.primary, ...(currentStreaks.secondary || [])]
    : currentStreaks.primary

  const filteredStreaks = displayStreaks.filter((s) => {
    const matchesContext = contextFilter === "all" || s.scope === contextFilter
    const matchesCategory = categoryFilter === "all" || s.category === categoryFilter
    return matchesContext && matchesCategory
  })

  const hasActiveFilters = contextFilter !== "all" || categoryFilter !== "all"

  if (displayStreaks.length === 0) {
    return null
  }

  const clearFilters = () => {
    setContextFilter("all")
    setCategoryFilter("all")
  }

  const groupedStreaks = {
    overall: filteredStreaks.filter((s) => !s.scope || s.scope === "overall"),
    home: filteredStreaks.filter((s) => s.scope === "home"),
    away: filteredStreaks.filter((s) => s.scope === "away"),
  }

  const hasContextSplits = groupedStreaks.home.length > 0 || groupedStreaks.away.length > 0

  return (
    <section ref={sectionRef} className="mb-16">
      {/* Section Header */}
      <div className="mb-8 flex items-center gap-6">
        <div className="relative group/logo">
          <div className="absolute inset-0 bg-gradient-to-br from-blue-500/30 to-cyan-500/30 rounded-3xl blur-2xl opacity-0 group-hover/logo:opacity-100 transition-opacity duration-500" />
          <div className="relative w-20 h-20 rounded-3xl bg-gradient-to-br from-blue-500/10 to-cyan-500/10 border-2 border-white/10 flex items-center justify-center hover:scale-110 transition-transform duration-500 backdrop-blur-sm">
            <img src={team.crestUrl || "/placeholder.svg"} alt={team.name} className="w-14 h-14 object-contain" />
          </div>
        </div>
        <div>
          <h2 className="text-2xl md:text-3xl lg:text-4xl font-black text-foreground mb-2 tracking-tight">
            Team Form
          </h2>
          <p className="text-sm md:text-base text-muted-foreground font-medium">
            The most important form signals right now
          </p>
        </div>
      </div>

      {/* Sticky Filter Bar */}
      <div
        ref={filterBarRef}
        className={`transition-all duration-200 ${
          isFilterSticky
            ? "sticky top-16 z-40 -mx-4 sm:-mx-6 lg:-mx-8 px-4 sm:px-6 lg:px-8 py-3 bg-background/95 backdrop-blur-sm border-b border-border/50 shadow-sm"
            : "mb-8"
        }`}
      >
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          {/* Context filters */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setContextFilter("all")}
              className={`px-3 sm:px-5 py-2 sm:py-2.5 rounded-xl text-xs sm:text-sm font-bold transition-all duration-300 ${
                contextFilter === "all"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30 scale-105"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted hover:scale-105"
              }`}
            >
              All
            </button>
            <button
              onClick={() => setContextFilter("home")}
              className={`px-3 sm:px-5 py-2 sm:py-2.5 rounded-xl text-xs sm:text-sm font-bold transition-all duration-300 ${
                contextFilter === "home"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30 scale-105"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted hover:scale-105"
              }`}
            >
              Home
            </button>
            <button
              onClick={() => setContextFilter("away")}
              className={`px-3 sm:px-5 py-2 sm:py-2.5 rounded-xl text-xs sm:text-sm font-bold transition-all duration-300 ${
                contextFilter === "away"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30 scale-105"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted hover:scale-105"
              }`}
            >
              Away
            </button>
          </div>

          {/* Category filters */}
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs sm:text-sm font-semibold text-muted-foreground mr-2">Category:</span>
            <button
              onClick={() => setCategoryFilter("all")}
              className={`px-3 sm:px-4 py-1.5 sm:py-2 rounded-lg text-xs sm:text-sm font-bold transition-all duration-300 ${
                categoryFilter === "all"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-md shadow-blue-500/20"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted"
              }`}
            >
              All
            </button>
            <button
              onClick={() => setCategoryFilter("result")}
              className={`px-3 sm:px-4 py-1.5 sm:py-2 rounded-lg text-xs sm:text-sm font-bold transition-all duration-300 ${
                categoryFilter === "result"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-md shadow-blue-500/20"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted"
              }`}
            >
              Results
            </button>
            <button
              onClick={() => setCategoryFilter("goal")}
              className={`px-3 sm:px-4 py-1.5 sm:py-2 rounded-lg text-xs sm:text-sm font-bold transition-all duration-300 ${
                categoryFilter === "goal"
                  ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-md shadow-blue-500/20"
                  : "bg-muted/50 text-muted-foreground hover:bg-muted"
              }`}
            >
              Goals
            </button>
          </div>
        </div>
      </div>

      {filteredStreaks.length === 0 && hasActiveFilters ? (
        <EmptyState
          title="No matching streaks"
          description={`No ${categoryFilter !== "all" ? categoryFilter + " " : ""}streaks found for ${contextFilter === "all" ? "all" : contextFilter} matches.`}
          onClearFilters={clearFilters}
        />
      ) : hasContextSplits && contextFilter === "all" ? (
        <div className="space-y-10">
          {groupedStreaks.overall.length > 0 && (
            <div>
              <h3 className="text-sm font-bold text-muted-foreground mb-5 flex items-center gap-3 uppercase tracking-wider">
                <span className="h-1.5 w-12 bg-gradient-to-r from-blue-500 to-transparent rounded-full" />
                Overall Form
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                {groupedStreaks.overall.map((streak) => (
                  <StreakMiniCard key={streak.streakId} streak={streak} />
                ))}
              </div>
            </div>
          )}

          {groupedStreaks.home.length > 0 && (
            <div>
              <h3 className="text-sm font-bold text-muted-foreground mb-5 flex items-center gap-3 uppercase tracking-wider">
                <span className="h-1.5 w-12 bg-gradient-to-r from-blue-500 to-transparent rounded-full" />
                Home Performance
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                {groupedStreaks.home.map((streak) => (
                  <StreakMiniCard key={streak.streakId} streak={streak} />
                ))}
              </div>
            </div>
          )}

          {groupedStreaks.away.length > 0 && (
            <div>
              <h3 className="text-sm font-bold text-muted-foreground mb-5 flex items-center gap-3 uppercase tracking-wider">
                <span className="h-1.5 w-12 bg-gradient-to-r from-purple-500 to-transparent rounded-full" />
                Away Performance
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
                {groupedStreaks.away.map((streak) => (
                  <StreakMiniCard key={streak.streakId} streak={streak} />
                ))}
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {filteredStreaks.map((streak) => (
            <StreakMiniCard key={streak.streakId} streak={streak} />
          ))}
        </div>
      )}
    </section>
  )
}
