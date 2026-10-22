"use client"

import Link from "next/link"
import { ArrowLeft, Trophy } from "lucide-react"
import type { LeagueContextFilter } from "@/types/league-page"

interface LeagueHeaderProps {
  leagueName: string
  leagueLogo: string
  season: string
  contextFilter: LeagueContextFilter
  onContextChange: (filter: LeagueContextFilter) => void
  availableContexts?: LeagueContextFilter[]
}

export function LeagueHeader({
  leagueName,
  leagueLogo,
  season,
  contextFilter,
  onContextChange,
  availableContexts = ["overall", "home", "away"],
}: LeagueHeaderProps) {
  return (
    <div className="sticky top-0 z-40 backdrop-blur-xl bg-background/80 border-b border-border/50">
      <div className="container mx-auto px-4 py-4 max-w-6xl">
        <div className="flex items-center justify-between mb-4">
          <Link
            href="/"
            className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            <span>Back to Home</span>
          </Link>
        </div>

        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
          <div className="flex items-center gap-4">
            <div className="relative">
              <img
                src={leagueLogo || "/placeholder.svg"}
                alt={leagueName}
                className="h-16 w-16 sm:h-20 sm:w-20 object-contain"
              />
            </div>
            <div>
              <h1 className="text-2xl sm:text-3xl md:text-4xl font-bold text-white mb-1">{leagueName}</h1>
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Trophy className="h-4 w-4" />
                <span>Season {season}</span>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 bg-background/50 backdrop-blur-sm border border-border/50 rounded-xl p-1">
            {availableContexts.map((filter) => (
              <button
                key={filter}
                onClick={() => onContextChange(filter)}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all duration-200 ${
                  contextFilter === filter
                    ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30"
                    : "text-muted-foreground hover:text-foreground hover:bg-white/5"
                }`}
              >
                {filter.charAt(0).toUpperCase() + filter.slice(1)}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
