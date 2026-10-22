"use client"

import { Target, Activity, Home, Plane } from "lucide-react"
import Image from "next/image"
import type { AvgGoalsStats } from "@/types/fixture-page"

type Team = {
  name: string
  logoUrl: string
  avgGoalsScored: number
  avgGoalsConceded: number
  avgGoals?: {
    overall: AvgGoalsStats
    home: AvgGoalsStats
    away: AvgGoalsStats
  }
}

type TeamAvgGoalsProps = {
  homeTeam: Team
  awayTeam: Team
}

type StatRowProps = {
  label: string
  icon: React.ReactNode
  scored: number
  conceded: number
  isHighlighted?: boolean
}

function StatRow({ label, icon, scored, conceded, isHighlighted }: StatRowProps) {
  return (
    <div className={`flex items-center gap-3 p-2.5 sm:p-3 rounded-lg transition-all ${
      isHighlighted
        ? "bg-primary/5 border border-primary/20"
        : "bg-background/30"
    }`}>
      <div className="flex items-center gap-2 w-20 sm:w-24 shrink-0">
        {icon}
        <span className={`text-xs sm:text-sm font-medium ${isHighlighted ? "text-foreground" : "text-muted-foreground"}`}>
          {label}
        </span>
      </div>
      <div className="flex-1 flex items-center justify-center gap-4 sm:gap-6">
        <div className="text-center">
          <p className={`text-lg sm:text-xl font-black tabular-nums ${isHighlighted ? "text-emerald-500" : "text-emerald-500/70"}`}>
            {scored.toFixed(1)}
          </p>
        </div>
        <div className="text-center">
          <p className={`text-lg sm:text-xl font-black tabular-nums ${isHighlighted ? "text-red-500" : "text-red-500/70"}`}>
            {conceded.toFixed(1)}
          </p>
        </div>
      </div>
    </div>
  )
}

type TeamStatCardProps = {
  team: Team
  variant: "home" | "away"
}

function TeamStatCard({ team, variant }: TeamStatCardProps) {
  const overallStats = team.avgGoals?.overall || { scored: team.avgGoalsScored, conceded: team.avgGoalsConceded }
  const contextStats = variant === "home"
    ? (team.avgGoals?.home || overallStats)
    : (team.avgGoals?.away || overallStats)

  const borderColor = variant === "home"
    ? "border-blue-500/30 hover:border-blue-500/50"
    : "border-purple-500/30 hover:border-purple-500/50"

  const contextLabel = variant === "home" ? "Home" : "Away"
  const ContextIcon = variant === "home" ? Home : Plane
  const iconColor = variant === "home" ? "text-blue-500" : "text-purple-500"

  return (
    <div className={`flex-1 glass-panel p-4 sm:p-5 rounded-xl border ${borderColor} transition-all duration-300`}>
      {/* Team Header */}
      <div className="flex items-center gap-3 mb-4">
        <Image
          src={team.logoUrl || "/placeholder.svg"}
          alt={team.name}
          width={40}
          height={40}
          className="w-10 h-10 sm:w-12 sm:h-12 object-contain"
        />
        <div className="min-w-0 flex-1">
          <p className="text-sm sm:text-base font-bold text-foreground truncate">
            {team.name}
          </p>
        </div>
      </div>

      {/* Column Headers */}
      <div className="flex items-center gap-3 mb-2 px-2.5 sm:px-3">
        <div className="w-20 sm:w-24 shrink-0" />
        <div className="flex-1 flex items-center justify-center gap-4 sm:gap-6">
          <p className="text-[10px] sm:text-xs text-muted-foreground font-medium uppercase tracking-wide text-center w-12">
            Scored
          </p>
          <p className="text-[10px] sm:text-xs text-muted-foreground font-medium uppercase tracking-wide text-center w-12">
            Conceded
          </p>
        </div>
      </div>

      {/* Stats Rows */}
      <div className="space-y-2">
        {/* Overall */}
        <StatRow
          label="Overall"
          icon={<Activity className="w-3.5 h-3.5 text-muted-foreground" />}
          scored={overallStats.scored}
          conceded={overallStats.conceded}
        />

        {/* Context (Home/Away) - Highlighted */}
        <StatRow
          label={contextLabel}
          icon={<ContextIcon className={`w-3.5 h-3.5 ${iconColor}`} />}
          scored={contextStats.scored}
          conceded={contextStats.conceded}
          isHighlighted
        />
      </div>
    </div>
  )
}

export function TeamAvgGoals({ homeTeam, awayTeam }: TeamAvgGoalsProps) {
  return (
    <div className="glass-panel-elevated p-4 sm:p-6 rounded-2xl animate-fade-in-up">
      {/* Header */}
      <div className="flex flex-col items-center gap-3 mb-5">
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-amber-500/20 to-orange-500/10 border border-amber-500/20 flex items-center justify-center">
          <Target className="h-5 w-5 text-amber-500" />
        </div>
        <div className="text-center">
          <h3 className="text-xl sm:text-2xl font-bold text-foreground">Average Goals</h3>
          <p className="text-xs sm:text-sm text-muted-foreground">Per match this season</p>
        </div>
      </div>

      {/* Team Stats */}
      <div className="flex flex-col sm:flex-row gap-3 sm:gap-4">
        <TeamStatCard team={homeTeam} variant="home" />
        <TeamStatCard team={awayTeam} variant="away" />
      </div>

      {/* Footer */}
      <div className="text-center text-[10px] sm:text-xs text-muted-foreground mt-4 pt-4 border-t border-border/30">
        Context stats highlighted for this fixture
      </div>
    </div>
  )
}
