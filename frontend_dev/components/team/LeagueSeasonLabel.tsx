import type { LeagueSeasonContext } from "@/types/team-page"

type LeagueSeasonLabelProps = {
  context: LeagueSeasonContext
}

export function LeagueSeasonLabel({ context }: LeagueSeasonLabelProps) {
  return (
    <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-muted/60 border border-border/50 backdrop-blur-sm">
      {context.countryName && (
        <>
          <span className="text-sm font-semibold text-foreground/80">{context.countryName}</span>
          <div className="w-1 h-1 rounded-full bg-muted-foreground/40" />
        </>
      )}
      <span className="text-sm font-semibold text-foreground">{context.leagueName}</span>
      <span className="text-sm text-muted-foreground font-medium">{context.seasonLabel}</span>
    </div>
  )
}
