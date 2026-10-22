"use client"

import { cn } from "@/lib/utils"
import { Globe } from "lucide-react"

interface LeagueOption {
  id: string
  label: string
  league_logo_url?: string | null
}

interface LeagueChipsProps {
  selected: string
  leagues: LeagueOption[]
  onChange: (leagueId: string) => void
  className?: string
}

export function LeagueChips({ selected, leagues, onChange, className }: LeagueChipsProps) {
  return (
    <div className={cn("flex gap-2 overflow-x-auto no-scrollbar pb-2 -mb-2", className)}>
      {leagues.map((league) => (
        <button
          key={league.id}
          onClick={() => onChange(league.id)}
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-all duration-200",
            "border",
            selected === league.id
              ? "bg-primary text-primary-foreground border-primary shadow-lg shadow-primary/25"
              : "bg-background hover:bg-muted border-border hover:border-primary/30",
          )}
        >
          {league.id === "all" ? (
            <Globe className="h-3.5 w-3.5 opacity-80" />
          ) : league.league_logo_url ? (
            <span className="flex h-4 w-4 items-center justify-center rounded-sm bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
              <img src={league.league_logo_url || "/placeholder.svg"} alt="" className="h-3 w-3 object-contain" />
            </span>
          ) : (
            <Globe className="h-3.5 w-3.5 opacity-70" />
          )}
          <span>{league.label}</span>
        </button>
      ))}
    </div>
  )
}
