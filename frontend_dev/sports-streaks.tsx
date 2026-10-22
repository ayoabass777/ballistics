"use client"

import { useState } from "react"
import { TrendingUp, Globe, Trophy, ArrowRight, Zap, BarChart3, Target } from "lucide-react"
import { StreakGrid } from "@/components/StreakGrid"
import { LoadingSkeleton } from "@/components/LoadingSkeleton"
import { AppHeader } from "@/components/home/AppHeader"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useHomeNavigation, useHomeStats, useStreakData, useStreakFilter } from "@/lib/hooks"

function formatCount(value: number): string {
  return Math.max(0, value).toLocaleString()
}

export default function SportsStreaks() {
  const [selectedLeague, setSelectedLeague] = useState<{ id: string; name: string; logo: string | null }>({
    id: "all",
    name: "All Leagues",
    logo: null,
  })
  const [headerSearchQuery, setHeaderSearchQuery] = useState("")
  const router = useRouter()

  // Use SWR hooks for data fetching
  const { streaks, loading, error } = useStreakData()
  const { leagueGroups, loading: navLoading, error: navError } = useHomeNavigation()
  const { stats: homeStats } = useHomeStats()
  const { filteredStreaks: leagueFilteredStreaks } = useStreakFilter(streaks, selectedLeague)
  const filteredStreaks = leagueFilteredStreaks

  const handleViewTeamStats = (teamName: string, fixtureId?: string) => {
    const returnUrl = encodeURIComponent("/")
    if (fixtureId) {
      router.push(`/fixture/${fixtureId}?returnTo=${returnUrl}`)
    } else {
      const teamSlug = teamName.toLowerCase().replace(/\s+/g, "-")
      router.push(`/team/${encodeURIComponent(teamSlug)}?returnTo=${returnUrl}`)
    }
  }

  const quickStats = [
    {
      icon: TrendingUp,
      value: formatCount(homeStats.activeStreaks),
      label: "Active Streaks",
      color: "text-blue-600 dark:text-blue-400",
    },
    {
      icon: Globe,
      value: formatCount(homeStats.leagues),
      label: "Leagues",
      color: "text-emerald-600 dark:text-emerald-400",
    },
    {
      icon: Trophy,
      value: formatCount(homeStats.teamsTracked),
      label: "Teams Tracked",
      color: "text-amber-600 dark:text-amber-400",
    },
  ]

  return (
    <div className="relative min-h-screen pb-16">
      <AppHeader
        searchQuery={headerSearchQuery}
        onSearchChange={setHeaderSearchQuery}
        selectedLeague={selectedLeague}
        onLeagueChange={setSelectedLeague}
        leagueGroups={leagueGroups}
        navLoading={navLoading}
        navError={navError}
      />

      {/* Hero CTA Section - Refined Design */}
      <div className="container mx-auto px-4 sm:px-6 pt-28 sm:pt-36 pb-8 sm:pb-12 max-w-5xl">
        <div className="relative overflow-hidden rounded-2xl sm:rounded-3xl glass-panel-elevated p-6 sm:p-10 md:p-12 group">
          {/* Subtle gradient overlay */}
          <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-accent/5 dark:from-primary/10 dark:via-transparent dark:to-accent/10" />

          {/* Decorative elements */}
          <div className="absolute top-0 right-0 w-48 sm:w-64 h-48 sm:h-64 bg-gradient-to-br from-primary/8 to-transparent dark:from-primary/15 rounded-full blur-3xl -translate-y-1/2 translate-x-1/4" />
          <div className="absolute bottom-0 left-0 w-40 sm:w-56 h-40 sm:h-56 bg-gradient-to-tr from-accent/8 to-transparent dark:from-accent/15 rounded-full blur-3xl translate-y-1/2 -translate-x-1/4" />

          <div className="relative z-10 text-center">
            {/* Badge */}
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 mb-5 sm:mb-6">
              <Zap className="h-3.5 w-3.5 text-primary" />
              <span className="text-xs font-semibold text-primary">Discover Match-Winning Patterns</span>
            </div>

            {/* Headline */}
            <h1 className="text-2xl sm:text-3xl md:text-4xl lg:text-5xl font-bold mb-4 sm:mb-5 text-foreground leading-tight tracking-tight">
              Unlock Winning Streak Insights
            </h1>

            {/* Subheadline */}
            <p className="text-sm sm:text-base md:text-lg text-muted-foreground mb-6 sm:mb-8 max-w-2xl mx-auto leading-relaxed">
              Comprehensive streak analytics across major leagues. Track form, identify patterns, and make informed decisions.
            </p>

            {/* CTA Button */}
            <Link
              href="/explore"
              className="inline-flex items-center gap-2.5 px-5 sm:px-7 py-3 sm:py-3.5 rounded-xl bg-primary text-primary-foreground font-semibold text-sm sm:text-base shadow-lg shadow-primary/25 hover:shadow-xl hover:shadow-primary/30 hover:bg-primary/90 active:scale-[0.98] transition-all duration-200 group/btn"
            >
              <BarChart3 className="h-4 w-4 sm:h-5 sm:w-5" />
              <span>Explore All Streaks</span>
              <ArrowRight className="h-4 w-4 sm:h-5 sm:w-5 transition-transform group-hover/btn:translate-x-0.5" />
            </Link>

            {/* Feature pills */}
            <div className="mt-6 sm:mt-8 flex flex-wrap justify-center gap-3 sm:gap-4">
              {[
                { icon: TrendingUp, text: `${formatCount(homeStats.activeStreaks)} Streaks` },
                { icon: Zap, text: "Real-time" },
                { icon: Target, text: "Advanced Filters" },
              ].map((item, i) => (
                <div
                  key={i}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-secondary/60 dark:bg-secondary/40 text-xs sm:text-sm text-muted-foreground"
                >
                  <item.icon className="h-3.5 w-3.5" />
                  <span>{item.text}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-4 sm:px-6 pb-16 sm:pb-20 max-w-7xl">
        {/* Quick Stats Section */}
        <div className="pb-10 sm:pb-14">
          <div className="grid grid-cols-3 gap-3 sm:gap-4 md:gap-5 mb-10 sm:mb-14">
            {quickStats.map((stat, index) => (
              <div
                key={index}
                className="glass-panel flex flex-col items-center gap-2 sm:gap-3 px-3 sm:px-5 py-4 sm:py-5 rounded-xl sm:rounded-2xl hover:border-primary/20 dark:hover:border-primary/30 transition-all duration-300 hover:scale-[1.02] group"
              >
                <div className="p-2 sm:p-2.5 rounded-xl bg-secondary/80 dark:bg-secondary/50 group-hover:bg-primary/10 dark:group-hover:bg-primary/20 transition-colors">
                  <stat.icon className={`h-4 w-4 sm:h-5 sm:w-5 ${stat.color}`} />
                </div>
                <div className="text-center">
                  <p className="text-xl sm:text-2xl md:text-3xl font-bold text-foreground tracking-tight">{stat.value}</p>
                  <p className="text-[11px] sm:text-xs text-muted-foreground font-medium mt-0.5">{stat.label}</p>
                </div>
              </div>
            ))}
          </div>

          {/* Top Streaks Section */}
          <div>
            <div className="flex flex-col sm:flex-row items-center justify-between mb-6 sm:mb-8 gap-3">
              <div className="text-center sm:text-left">
                <h2 className="text-2xl sm:text-3xl font-bold text-foreground mb-1">
                  {selectedLeague.id === "all" ? "Top Streaks" : `${selectedLeague.name} Streaks`}
                </h2>
                <p className="text-sm text-muted-foreground">
                  {filteredStreaks.length} active streak{filteredStreaks.length !== 1 ? "s" : ""} tracked
                </p>
              </div>
            </div>

            {loading ? (
              <LoadingSkeleton />
            ) : (
              <StreakGrid streaks={filteredStreaks} loading={loading} error={error} onViewStats={handleViewTeamStats} />
            )}

            {!loading && filteredStreaks.length > 0 && (
              <div className="flex justify-center mt-10 sm:mt-14">
                <Link
                  href="/explore"
                  className="inline-flex items-center gap-2.5 px-6 sm:px-8 py-3.5 sm:py-4 rounded-xl glass-panel hover:border-primary/30 dark:hover:border-primary/40 text-foreground font-semibold text-sm sm:text-base hover:scale-[1.02] active:scale-[0.98] transition-all duration-200 group/btn"
                >
                  <TrendingUp className="h-5 w-5 text-primary transition-transform group-hover/btn:rotate-6" />
                  <span>Explore More Streaks</span>
                  <ArrowRight className="h-5 w-5 text-muted-foreground transition-transform group-hover/btn:translate-x-0.5" />
                </Link>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
