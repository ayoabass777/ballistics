"use client"

import { useState } from "react"
import { LeagueHeader } from "./LeagueHeader"
import { LeagueTable } from "./LeagueTable"
import { LeagueStreaks } from "./LeagueStreaks"
import type { LeagueContextFilter, LeaguePageViewModel } from "@/types/league-page"

export function LeaguePageShell({ leagueData }: { leagueData: LeaguePageViewModel }) {
  const [contextFilter, setContextFilter] = useState<LeagueContextFilter>(leagueData.availableContexts[0] || "overall")

  const currentStandings = leagueData.standings[contextFilter] || leagueData.standings.overall

  return (
    <div className="min-h-screen bg-background">
      <LeagueHeader
        leagueName={leagueData.leagueName}
        leagueLogo={leagueData.leagueLogo || "/placeholder.svg"}
        season={leagueData.season}
        contextFilter={contextFilter}
        onContextChange={setContextFilter}
        availableContexts={leagueData.availableContexts}
      />

      <div className="container mx-auto px-4 py-8 max-w-6xl">
        <LeagueStreaks
          leagueSlug={leagueData.leagueId}
          leagueDisplayName={leagueData.leagueName}
          countryName={leagueData.countryName}
          streaks={leagueData.topStreaks}
        />
        <LeagueTable standings={currentStandings} contextFilter={contextFilter} />
      </div>
    </div>
  )
}
