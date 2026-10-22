"use client"

import { TrendingUp, Flame, Shield, Target } from "lucide-react"
import Link from "next/link"
import type { LeagueTopStreak } from "@/types/league-page"

interface LeagueStreaksProps {
  leagueSlug: string
  leagueDisplayName: string
  countryName?: string
  streaks: LeagueTopStreak[]
}

function toSlug(value: string | null | undefined): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
  return normalized || "item"
}

function toLeagueFilterId(leagueName: string, countryName?: string | null): string {
  return `${toSlug(countryName || "unknown")}-${toSlug(leagueName)}`
}

function getStreakIcon(metricKey: string) {
  if (metricKey === "win" || metricKey === "unbeaten") {
    return <TrendingUp className="h-4 w-4" />
  }
  if (metricKey === "clean_sheet") {
    return <Shield className="h-4 w-4" />
  }
  if (metricKey.includes("goal") || metricKey.includes("score")) {
    return <Target className="h-4 w-4" />
  }
  return <Flame className="h-4 w-4" />
}

function getStreakColor(metricKey: string) {
  if (metricKey === "win") {
    return "text-emerald-600 dark:text-emerald-400 bg-emerald-500/10 border-emerald-500/20"
  }
  if (metricKey === "loss") {
    return "text-red-600 dark:text-red-400 bg-red-500/10 border-red-500/20"
  }
  if (metricKey === "clean_sheet") {
    return "text-blue-600 dark:text-blue-400 bg-blue-500/10 border-blue-500/20"
  }
  if (metricKey.includes("goal") || metricKey.includes("score")) {
    return "text-amber-600 dark:text-amber-400 bg-amber-500/10 border-amber-500/20"
  }
  return "text-primary bg-primary/10 border-primary/20"
}

export function LeagueStreaks({ leagueSlug, leagueDisplayName, countryName, streaks }: LeagueStreaksProps) {
  const leagueStreaks = [...streaks]
    .sort((a, b) => b.currentStreakLength - a.currentStreakLength)
    .slice(0, 6)
  const leagueFilterId = toLeagueFilterId(leagueDisplayName, countryName)

  if (leagueStreaks.length === 0) {
    return null
  }

  return (
    <div className="mb-8">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded-lg bg-primary/10 border border-primary/20">
            <Flame className="h-4 w-4 text-primary" />
          </div>
          <h2 className="text-lg font-bold text-foreground">Hot Streaks</h2>
        </div>
        <Link
          href={`/explore?league=${encodeURIComponent(leagueFilterId)}`}
          className="text-xs font-medium text-primary hover:text-primary/80 transition-colors"
        >
          View all
        </Link>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {leagueStreaks.map((streak) => (
          <StreakCard key={streak.streakId} streak={streak} leagueSlug={leagueSlug} />
        ))}
      </div>
    </div>
  )
}

function StreakCard({ streak, leagueSlug }: { streak: LeagueTopStreak; leagueSlug: string }) {
  const colorClasses = getStreakColor(streak.metricKey)

  return (
    <Link
      href={streak.nextFixtureId ? `/fixture/${streak.nextFixtureId}?returnTo=${encodeURIComponent(`/league/${leagueSlug}`)}` : "#"}
      className="group glass-panel p-3 rounded-xl hover:border-primary/30 transition-all duration-200"
    >
      <div className="flex items-start gap-3">
        {/* Team logo */}
        <div className="flex-shrink-0">
          {streak.teamLogoUrl ? (
            <img
              src={streak.teamLogoUrl}
              alt={streak.teamName}
              className="h-10 w-10 object-contain"
            />
          ) : (
            <div className="h-10 w-10 rounded-full bg-secondary flex items-center justify-center">
              <span className="text-sm font-bold text-muted-foreground">
                {streak.teamName.charAt(0)}
              </span>
            </div>
          )}
        </div>

        {/* Streak info */}
        <div className="flex-1 min-w-0">
          <p className="font-semibold text-sm text-foreground truncate group-hover:text-primary transition-colors">
            {streak.teamName}
          </p>
          <div className="flex items-center gap-2 mt-1">
            <span
              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium border ${colorClasses}`}
            >
              {getStreakIcon(streak.metricKey)}
              <span>{streak.currentStreakLength}</span>
            </span>
            <span className="text-xs text-muted-foreground truncate">
              {streak.headline}
            </span>
          </div>
        </div>
      </div>

      {/* Next match */}
      {streak.nextOpponentName && (
        <div className="mt-2 pt-2 border-t border-border/50">
          <p className="text-xs text-muted-foreground">
            Next: <span className="text-foreground font-medium">vs {streak.nextOpponentName}</span>
          </p>
        </div>
      )}
    </Link>
  )
}
