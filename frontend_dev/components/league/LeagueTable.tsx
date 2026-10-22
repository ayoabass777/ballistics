"use client"

import Link from "next/link"
import type { LeagueContextFilter, LeagueTableRow } from "@/types/league-page"

interface LeagueTableProps {
  standings: LeagueTableRow[]
  contextFilter: LeagueContextFilter
}

export function LeagueTable({ standings, contextFilter }: LeagueTableProps) {
  const statValue = (value: number | null | undefined, opts?: { signed?: boolean }) => {
    if (value === null || value === undefined) return "—"
    if (opts?.signed && value > 0) return `+${value}`
    return String(value)
  }

  const getPositionColor = (position?: number | null) => {
    return "border-l-4 border-transparent"
  }

  const getFormIcon = (result: string) => {
    switch (result) {
      case "W":
        return (
          <div className="w-6 h-6 rounded-full bg-green-500/20 flex items-center justify-center text-green-400 text-xs font-bold">
            W
          </div>
        )
      case "D":
        return (
          <div className="w-6 h-6 rounded-full bg-yellow-500/20 flex items-center justify-center text-yellow-400 text-xs font-bold">
            D
          </div>
        )
      case "L":
        return (
          <div className="w-6 h-6 rounded-full bg-red-500/20 flex items-center justify-center text-red-400 text-xs font-bold">
            L
          </div>
        )
      default:
        return null
    }
  }

  return (
    <div className="rounded-xl bg-gradient-to-br from-slate-900/50 to-slate-800/30 backdrop-blur-sm border border-white/10 overflow-hidden">
      {/* Desktop Table */}
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-white/10 text-sm text-muted-foreground">
              <th className="text-left py-4 px-4 font-semibold w-12">#</th>
              <th className="text-left py-4 px-4 font-semibold">Team</th>
              <th className="text-center py-4 px-4 font-semibold w-16">P</th>
              <th className="text-center py-4 px-4 font-semibold w-16">W</th>
              <th className="text-center py-4 px-4 font-semibold w-16">D</th>
              <th className="text-center py-4 px-4 font-semibold w-16">L</th>
              <th className="text-center py-4 px-4 font-semibold w-20">GF</th>
              <th className="text-center py-4 px-4 font-semibold w-20">GA</th>
              <th className="text-center py-4 px-4 font-semibold w-20">GD</th>
              <th className="text-center py-4 px-4 font-semibold w-20">PTS</th>
              <th className="text-center py-4 px-4 font-semibold">Form</th>
            </tr>
          </thead>
          <tbody>
            {standings.map((team, index) => (
              <tr
                key={team.teamId}
                className={`border-b border-white/5 hover:bg-white/5 transition-colors ${getPositionColor(team.position)}`}
              >
                <td className="py-4 px-4 font-bold text-sm">{statValue(team.position)}</td>
                <td className="py-4 px-4">
                  <Link
                    href={`/team/${team.teamSlug || team.teamId}`}
                    className="flex items-center gap-3 hover:text-blue-400 transition-colors group"
                  >
                    <img
                      src={team.teamLogo || "/placeholder.svg"}
                      alt={team.teamName}
                      className="h-8 w-8 object-contain group-hover:scale-110 transition-transform"
                    />
                    <span className="font-medium text-white">{team.teamName}</span>
                  </Link>
                </td>
                <td className="py-4 px-4 text-center text-muted-foreground text-sm">{statValue(team.played)}</td>
                <td className="py-4 px-4 text-center text-green-400 text-sm font-medium">{statValue(team.wins)}</td>
                <td className="py-4 px-4 text-center text-yellow-400 text-sm font-medium">{statValue(team.draws)}</td>
                <td className="py-4 px-4 text-center text-red-400 text-sm font-medium">{statValue(team.losses)}</td>
                <td className="py-4 px-4 text-center text-muted-foreground text-sm">{statValue(team.goalsFor)}</td>
                <td className="py-4 px-4 text-center text-muted-foreground text-sm">{statValue(team.goalsAgainst)}</td>
                <td
                  className={`py-4 px-4 text-center text-sm font-medium ${
                    team.goalDifference === null || team.goalDifference === undefined
                      ? "text-muted-foreground"
                      : team.goalDifference >= 0
                        ? "text-green-400"
                        : "text-red-400"
                  }`}
                >
                  {statValue(team.goalDifference, { signed: true })}
                </td>
                <td className="py-4 px-4 text-center text-white text-base font-bold">{statValue(team.points)}</td>
                <td className="py-4 px-4">
                  <div className="flex items-center justify-center gap-1">
                    {team.form.slice(-5).map((result, i) => (
                      <div key={i}>{getFormIcon(result)}</div>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile Cards */}
      <div className="md:hidden">
        {standings.map((team) => (
          <div key={team.teamId} className={`p-4 border-b border-white/5 ${getPositionColor(team.position)}`}>
            <div className="flex items-center justify-between mb-3">
              <Link href={`/team/${team.teamSlug || team.teamId}`} className="flex items-center gap-3 flex-1">
                <span className="text-sm font-bold text-muted-foreground w-6">{statValue(team.position)}</span>
                <img src={team.teamLogo || "/placeholder.svg"} alt={team.teamName} className="h-8 w-8 object-contain" />
                <span className="font-medium text-white">{team.teamName}</span>
              </Link>
              <span className="text-lg font-bold text-white">{statValue(team.points)}</span>
            </div>
            <div className="flex items-center justify-between text-xs">
              <div className="flex gap-4 text-muted-foreground">
                <span>P: {statValue(team.played)}</span>
                <span className="text-green-400">W: {statValue(team.wins)}</span>
                <span className="text-yellow-400">D: {statValue(team.draws)}</span>
                <span className="text-red-400">L: {statValue(team.losses)}</span>
              </div>
              <div className="flex gap-1">
                {team.form.slice(-5).map((result, i) => (
                  <div key={i} className="scale-75">
                    {getFormIcon(result)}
                  </div>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>

    </div>
  )
}
