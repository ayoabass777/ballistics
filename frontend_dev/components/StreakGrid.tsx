"use client"

import { Trophy, XCircle } from "lucide-react"
import { StreakCard } from "@/components/utils/StreakCard"
import { type StreakData } from "@/lib/mock-streaks"

interface StreakGridProps {
  streaks: StreakData[]
  loading: boolean
  error: string | null
  onViewStats: (teamName: string, fixtureId?: string) => void
}

function getSecondaryStreak(streak: StreakData): string | undefined {
  // Auto-generate reinforcing secondary streaks based on metric key
  if (streak.metric_key === "win" && streak.current_streak_length >= 3) {
    return "Building momentum"
  }
  if (streak.metric_key === "loss" && streak.current_streak_length >= 3) {
    return "Under pressure"
  }
  if (streak.metric_key === "clean_sheet" && streak.current_streak_length >= 2) {
    return "Solid at the back"
  }
  return undefined
}

function getRecentForm(streak: StreakData): Array<"W" | "D" | "L"> | undefined {
  if (streak.recent_form && streak.recent_form.length > 0) return streak.recent_form
  return undefined
}

export function StreakGrid({ streaks, loading, error, onViewStats }: StreakGridProps) {
  if (loading) {
    return null
  }

  if (error) {
    return (
      <div className="glass-panel p-12 text-center animate-fade-in">
        <XCircle className="h-12 w-12 text-destructive mx-auto mb-4" />
        <h3 className="text-xl font-bold text-foreground mb-2">Failed to load streaks</h3>
        <p className="text-muted-foreground">{error}</p>
      </div>
    )
  }

  if (streaks.length === 0) {
    return (
      <div className="glass-panel p-12 text-center animate-fade-in">
        <div className="bg-muted/30 p-4 rounded-full w-16 h-16 flex items-center justify-center mx-auto mb-4">
          <Trophy className="h-8 w-8 text-muted-foreground" />
        </div>
        <h3 className="text-xl font-bold text-foreground mb-2">No streaks found</h3>
        <p className="text-muted-foreground">Try adjusting your search or filters</p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 xl:grid-cols-3 gap-6">
      {streaks.map((streak, index) => (
        <StreakCard
          key={`${streak.streak_id}-${index}`}
          index={index}
          teamName={streak.team_name}
          teamLogoUrl={streak.team_logo_url}
          leagueName={streak.league_name}
          countryName={streak.country_name}
          countryLogoUrl={streak.country_logo_url}
          leagueLogoUrl={streak.league_logo_url}
          headline={streak.headline}
          secondaryStreak={getSecondaryStreak(streak)}
          recentForm={getRecentForm(streak)}
          scope={streak.scope}
          nextOpponent={streak.next_opponent}
          nextIsHome={streak.next_is_home}
          nextMatchDate={streak.next_match_date}
          onViewStats={() => onViewStats(streak.team_name, streak.fixture_id)}
          confidenceScore={streak.confidence_score}
          teamTier={streak.team_tier}
          opponentTier={streak.opponent_tier}
        />
      ))}
    </div>
  )
}
