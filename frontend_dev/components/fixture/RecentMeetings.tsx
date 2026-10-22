"use client"

import { Home, Plane } from "lucide-react"
import Image from "next/image"
import { useState } from "react"

type FormMatch = {
  date: string
  opponent: string
  score: string
  result: "W" | "L" | "D"
  isHome: boolean
}

type Team = {
  name: string
  logoUrl: string
}

interface RecentMeetingsProps {
  recentMatches: Array<{
    date: string
    home: number
    away: number
    result: "H" | "A" | "D"
    isHomeTeamHome?: boolean
  }>
  homeTeamForm: FormMatch[]
  awayTeamForm: FormMatch[]
  homeTeam: Team
  awayTeam: Team
}

type TabType = "home" | "h2h" | "away"
type ContextFilter = "home" | "away" | null

type FormMatchRowProps = {
  match: FormMatch
  team: Team
  index: number
  variant: "home" | "away"
}

function FormMatchRow({ match, team, index, variant }: FormMatchRowProps) {
  const resultStyles = {
    W: "bg-emerald-500/20 text-emerald-500 dark:text-emerald-400 border border-emerald-500/30",
    L: "bg-red-500/20 text-red-500 dark:text-red-400 border border-red-500/30",
    D: "bg-muted/30 text-muted-foreground border border-border/40",
  }

  const venueIconColor = variant === "home"
    ? "text-cyan-500 dark:text-cyan-400"
    : "text-purple-500 dark:text-purple-400"

  return (
    <div
      className="flex items-center p-2.5 sm:p-3 rounded-lg bg-background/30 hover:bg-background/50 border border-border/20 hover:border-border/40 transition-all duration-200 group animate-fade-in-up"
      style={{ animationDelay: `${index * 30}ms` }}
    >
      <div className="flex items-center gap-2 flex-1 min-w-0">
        <span className="text-[10px] sm:text-xs text-muted-foreground font-medium w-12 sm:w-16 shrink-0">
          {new Date(match.date).toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
          })}
        </span>
        <Image
          src={team.logoUrl || "/placeholder.svg"}
          alt={team.name}
          width={20}
          height={20}
          className="w-5 h-5 object-contain shrink-0"
        />
        <span className="text-xs sm:text-sm text-foreground truncate">{match.opponent}</span>
      </div>

      <div className="flex items-center gap-1.5 sm:gap-2 shrink-0">
        <span className="font-mono text-sm sm:text-base font-bold text-foreground min-w-[45px] sm:min-w-[55px] text-center tabular-nums">
          {match.score}
        </span>
        <div className={`w-7 h-7 sm:w-8 sm:h-8 rounded-md flex items-center justify-center font-black text-[10px] sm:text-xs transition-all duration-200 ${resultStyles[match.result]}`}>
          {match.result}
        </div>
        {match.isHome ? (
          <Home className="h-3.5 w-3.5 text-blue-500 dark:text-blue-400" />
        ) : (
          <Plane className={`h-3.5 w-3.5 ${venueIconColor}`} />
        )}
      </div>
    </div>
  )
}

export function RecentMeetings({ recentMatches, homeTeamForm, awayTeamForm, homeTeam, awayTeam }: RecentMeetingsProps) {
  const initialTab: TabType = recentMatches.length > 0
    ? "h2h"
    : homeTeamForm.length > 0
      ? "home"
      : "away"
  const [activeTab, setActiveTab] = useState<TabType>(initialTab)
  const [homeContext, setHomeContext] = useState<ContextFilter>(null)
  const [awayContext, setAwayContext] = useState<ContextFilter>(null)
  const [h2hSameVenue, setH2hSameVenue] = useState(false)

  const filteredHomeForm = homeTeamForm.filter((match) => {
    if (homeContext === null) return true
    if (homeContext === "home") return match.isHome
    return true
  })

  const filteredAwayForm = awayTeamForm.filter((match) => {
    if (awayContext === null) return true
    if (awayContext === "away") return !match.isHome
    return true
  })

  const filteredH2HMatches = recentMatches.filter((match) => {
    if (!h2hSameVenue) return true
    return match.isHomeTeamHome === true
  })

  const toggleContext = (
    setter: (val: ContextFilter) => void,
    currentValue: ContextFilter,
    targetValue: ContextFilter,
  ) => {
    if (currentValue === targetValue) {
      setter(null)
    } else {
      setter(targetValue)
    }
  }

  return (
    <div className="glass-panel-elevated p-4 sm:p-6 animate-fade-in-up">
      {/* Section header */}
      <div className="text-center mb-5">
        <h3 className="text-lg sm:text-xl font-bold text-foreground mb-1">Recent Matches</h3>
        <p className="text-xs sm:text-sm text-muted-foreground">Form and head-to-head history</p>
      </div>

      {/* Tab navigation */}
      <div className="flex justify-center items-center gap-2 sm:gap-3 mb-5">
        <button
          onClick={() => setActiveTab("home")}
          className={`
            flex items-center justify-center p-2 sm:p-3 rounded-xl transition-all duration-300
            ${
              activeTab === "home"
                ? "bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border-2 border-blue-500/40 scale-105 shadow-lg shadow-blue-500/10"
                : "bg-background/40 border border-border/30 hover:bg-background/60 hover:scale-105"
            }
          `}
        >
          <Image
            src={homeTeam.logoUrl || "/placeholder.svg"}
            alt={homeTeam.name}
            width={36}
            height={36}
            className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
          />
        </button>

        <button
          onClick={() => setActiveTab("h2h")}
          className={`
            flex items-center justify-center px-4 sm:px-6 py-2 sm:py-3 rounded-xl font-bold text-xs sm:text-sm transition-all duration-300 min-w-[70px] sm:min-w-[90px]
            ${
              activeTab === "h2h"
                ? "bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border-2 border-blue-500/40 text-blue-500 dark:text-blue-400 scale-105 shadow-lg shadow-blue-500/10"
                : "bg-background/40 border border-border/30 text-muted-foreground hover:bg-background/60 hover:scale-105"
            }
          `}
        >
          H2H
        </button>

        <button
          onClick={() => setActiveTab("away")}
          className={`
            flex items-center justify-center p-2 sm:p-3 rounded-xl transition-all duration-300
            ${
              activeTab === "away"
                ? "bg-gradient-to-br from-purple-500/20 to-pink-500/10 border-2 border-purple-500/40 scale-105 shadow-lg shadow-purple-500/10"
                : "bg-background/40 border border-border/30 hover:bg-background/60 hover:scale-105"
            }
          `}
        >
          <Image
            src={awayTeam.logoUrl || "/placeholder.svg"}
            alt={awayTeam.name}
            width={36}
            height={36}
            className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
          />
        </button>
      </div>

      {/* Context filters */}
      {activeTab === "home" && (
        <div className="flex justify-center gap-2 mb-4">
          <button
            onClick={() => toggleContext(setHomeContext, homeContext, "home")}
            className={`
              p-2 rounded-lg transition-all duration-200
              ${
                homeContext === "home"
                  ? "bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border-2 border-blue-500/40 text-blue-500 dark:text-blue-400"
                  : "bg-background/40 border border-border/30 text-muted-foreground hover:bg-background/60"
              }
            `}
            title="Home matches only"
          >
            <Home className="h-4 w-4" />
          </button>
        </div>
      )}

      {activeTab === "h2h" && (
        <div className="flex justify-center gap-2 mb-4">
          <button
            onClick={() => setH2hSameVenue(!h2hSameVenue)}
            className={`
              flex items-center gap-1.5 px-3 py-2 rounded-lg transition-all duration-200 text-xs sm:text-sm
              ${
                h2hSameVenue
                  ? "bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border-2 border-blue-500/40 text-blue-500 dark:text-blue-400"
                  : "bg-background/40 border border-border/30 text-muted-foreground hover:bg-background/60"
              }
            `}
            title={`Matches at ${homeTeam.name}'s stadium`}
          >
            <Home className="h-3.5 w-3.5" />
            <span className="font-medium">Same venue</span>
          </button>
        </div>
      )}

      {activeTab === "away" && (
        <div className="flex justify-center gap-2 mb-4">
          <button
            onClick={() => toggleContext(setAwayContext, awayContext, "away")}
            className={`
              p-2 rounded-lg transition-all duration-200
              ${
                awayContext === "away"
                  ? "bg-gradient-to-br from-purple-500/20 to-pink-500/10 border-2 border-purple-500/40 text-purple-500 dark:text-purple-400"
                  : "bg-background/40 border border-border/30 text-muted-foreground hover:bg-background/60"
              }
            `}
            title="Away matches only"
          >
            <Plane className="h-4 w-4" />
          </button>
        </div>
      )}

      <div className="space-y-2">
        {activeTab === "h2h" &&
          filteredH2HMatches.map((match, idx) => {
            // For each historical fixture row, left is that match's home team,
            // right is that match's away team.
            const leftTeam = match.isHomeTeamHome === false ? awayTeam : homeTeam
            const rightTeam = match.isHomeTeamHome === false ? homeTeam : awayTeam
            const leftScore = match.home
            const rightScore = match.away

            const leftWon = leftScore > rightScore
            const rightWon = rightScore > leftScore

            return (
              <div
                key={idx}
                className="grid grid-cols-[1fr_auto_1fr] items-center p-2.5 sm:p-3 rounded-lg bg-background/30 hover:bg-background/50 border border-border/20 hover:border-border/40 transition-all duration-200 group animate-fade-in-up"
                style={{ animationDelay: `${idx * 30}ms` }}
              >
                {/* Left: Date + Home team logo */}
                <div className="flex items-center gap-2 sm:gap-3">
                  <span className="text-[10px] sm:text-xs text-muted-foreground font-medium w-12 sm:w-16 shrink-0">
                    {new Date(match.date).toLocaleDateString("en-US", {
                      month: "short",
                      day: "numeric",
                    })}
                  </span>
                  <Image
                    src={leftTeam.logoUrl || "/placeholder.svg"}
                    alt={leftTeam.name}
                    width={24}
                    height={24}
                    className={`w-5 h-5 sm:w-6 sm:h-6 object-contain transition-opacity duration-200 ${rightWon ? "opacity-40" : ""}`}
                  />
                </div>

                {/* Center: Scoreline (always centered) */}
                <div className="flex items-center justify-center gap-2 px-3 sm:px-4">
                  <span
                    className={`font-black text-base sm:text-lg font-mono tabular-nums transition-colors duration-200 ${
                      rightWon ? "text-muted-foreground/50" : "text-foreground"
                    }`}
                  >
                    {leftScore}
                  </span>
                  <span className="text-xs text-muted-foreground font-medium">-</span>
                  <span
                    className={`font-black text-base sm:text-lg font-mono tabular-nums transition-colors duration-200 ${
                      leftWon ? "text-muted-foreground/50" : "text-foreground"
                    }`}
                  >
                    {rightScore}
                  </span>
                </div>

                {/* Right: Away team logo */}
                <div className="flex items-center justify-end">
                  <Image
                    src={rightTeam.logoUrl || "/placeholder.svg"}
                    alt={rightTeam.name}
                    width={24}
                    height={24}
                    className={`w-5 h-5 sm:w-6 sm:h-6 object-contain transition-opacity duration-200 ${leftWon ? "opacity-40" : ""}`}
                  />
                </div>
              </div>
            )
          })}

        {activeTab === "home" &&
          filteredHomeForm.map((match, idx) => (
            <FormMatchRow key={idx} match={match} team={homeTeam} index={idx} variant="home" />
          ))}

        {activeTab === "away" &&
          filteredAwayForm.map((match, idx) => (
            <FormMatchRow key={idx} match={match} team={awayTeam} index={idx} variant="away" />
          ))}
      </div>
    </div>
  )
}
