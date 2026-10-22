import Image from "next/image"
import type { TeamIdentity, LeagueSeasonContext } from "@/types/team-page"
import { LeagueSeasonLabel } from "./LeagueSeasonLabel"
import { LeagueTeamSwitcher } from "./LeagueTeamSwitcher"
import { FormDots } from "@/components/ui/FormDots"

type TeamHeaderProps = {
  team: TeamIdentity
  context: LeagueSeasonContext
  backToLeagueHref: string
  exploreStreaksHref: string
  formLastN?: Array<"W" | "D" | "L">
}

export function TeamHeader({ team, context, formLastN }: TeamHeaderProps) {
  return (
    <div className="glass-panel-elevated p-6 md:p-12 mb-8 md:mb-10 relative overflow-hidden group">
      <div className="absolute inset-0 bg-gradient-to-br from-blue-500/5 via-transparent to-cyan-500/5 opacity-0 group-hover:opacity-100 transition-opacity duration-500" />

      <div className="mb-10 relative z-10">
        <LeagueSeasonLabel context={context} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto] gap-10 items-start relative z-10">
        <div className="flex items-center gap-8">
          {team.crestUrl && (
            <div className="relative shrink-0">
              <div className="absolute -top-3 -right-3 z-20 bg-gradient-to-br from-blue-500 via-blue-600 to-cyan-500 text-white text-sm font-black px-3.5 py-1.5 rounded-full shadow-2xl border-3 border-background ring-2 ring-blue-500/30">
                {team.rank}
              </div>

              <div className="relative w-24 h-24 md:w-32 md:h-32 rounded-3xl p-4 md:p-5 flex items-center justify-center hover:scale-110 transition-all duration-500 group/logo">
                <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-cyan-500/10 rounded-3xl opacity-0 group-hover/logo:opacity-100 transition-opacity blur-xl" />
                <Image
                  src={team.crestUrl || "/placeholder.svg"}
                  alt={team.name}
                  width={96}
                  height={96}
                  className="object-contain relative z-10"
                />
              </div>
            </div>
          )}

          <div className="flex-1 min-w-0">
            <h1 className="text-2xl md:text-4xl lg:text-5xl font-black text-foreground mb-3 leading-tight tracking-tight bg-clip-text">
              {team.name}
            </h1>
            <div className="flex items-center gap-3">
              <span className="text-sm text-muted-foreground font-medium">Last 5:</span>
              {formLastN && formLastN.length > 0 ? (
                <FormDots form={formLastN.slice(0, 5)} size="md" />
              ) : (
                <span className="text-sm text-muted-foreground">No form data</span>
              )}
            </div>
          </div>
        </div>

        <div className="flex flex-col gap-4 lg:items-end">
          <LeagueTeamSwitcher currentTeam={team} context={context} />
        </div>
      </div>
    </div>
  )
}
