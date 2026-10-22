"use client"

import { useState } from "react"
import Link from "next/link"
import { ChevronDown, Check, Users } from "lucide-react"
import type { LeagueSeasonContext, TeamIdentity } from "@/types/team-page"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"

type LeagueTeamSwitcherProps = {
  currentTeam: TeamIdentity
  context: LeagueSeasonContext
}

export function LeagueTeamSwitcher({ currentTeam, context }: LeagueTeamSwitcherProps) {
  const [open, setOpen] = useState(false)

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger className="group flex items-center gap-2.5 px-4 py-2.5 rounded-xl border-2 border-border hover:border-primary/50 bg-background hover:bg-accent transition-all duration-200 font-medium shadow-sm hover:shadow-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2">
        <Users className="w-5 h-5 text-muted-foreground group-hover:text-primary transition-colors" />
        <span className="text-sm">Switch Team</span>
        <ChevronDown className="w-4 h-4 text-muted-foreground transition-transform group-data-[state=open]:rotate-180" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72 p-2">
        <div className="px-3 py-2 mb-1">
          <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
            {context.leagueName} Teams
          </p>
        </div>
        {context.leagueTeams.map((team) => {
          const isActive = team.teamId === currentTeam.teamId
          return (
            <DropdownMenuItem key={team.teamId} asChild>
              <Link
                href={team.teamHref}
                className={`flex items-center justify-between cursor-pointer px-3 py-2.5 rounded-lg transition-colors ${
                  isActive ? "bg-primary/10 text-primary font-semibold" : "hover:bg-muted"
                }`}
              >
                <span className="text-sm">{team.name}</span>
                {isActive && <Check className="w-4 h-4 text-primary" />}
              </Link>
            </DropdownMenuItem>
          )
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
