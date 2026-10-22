import Link from "next/link"
import { Calendar, ArrowRight } from "lucide-react"
import type { UpcomingMatchVM } from "@/types/team-page"
import { MatchCountdown } from "@/components/ui/MatchCountdown"

type UpcomingMatchBadgeProps = {
  upcomingMatch: UpcomingMatchVM
  teamSlug: string
}

export function UpcomingMatchBadge({ upcomingMatch, teamSlug }: UpcomingMatchBadgeProps) {
  const { isHome, opponent, kickoffIso, venue } = upcomingMatch

  const fixtureId = upcomingMatch.matchId
  const returnTo = encodeURIComponent(`/team/${teamSlug}`)
  const fixtureHref = `/fixture/${fixtureId}?returnTo=${returnTo}`

  return (
    <div className="my-10">
      <Link
        href={fixtureHref}
        className="block group max-w-3xl mx-auto p-8 rounded-2xl border-2 border-border/50 bg-gradient-to-br from-card to-card/50 hover:border-blue-500/50 hover:shadow-2xl hover:shadow-blue-500/10 transition-all duration-500 backdrop-blur-sm cursor-pointer"
      >
        <div className="flex items-center gap-6">
          <div className="p-3 rounded-xl bg-gradient-to-br from-blue-500/10 to-cyan-500/10 border border-blue-500/20">
            <Calendar className="w-6 h-6 text-blue-400" />
          </div>

          <div className="flex-1">
            <div className="flex items-center gap-2 text-xs sm:text-sm font-semibold text-muted-foreground mb-2">
              {kickoffIso ? (
                <MatchCountdown matchDate={kickoffIso} />
              ) : (
                <span>Upcoming match</span>
              )}
              {venue && <span>• {venue}</span>}
            </div>
            <div className="text-xl sm:text-2xl font-black text-foreground tracking-tight">
              {isHome ? "vs" : "@"} {opponent.name}
            </div>
          </div>

          {opponent.crestUrl && (
            <div className="relative shrink-0">
              {opponent.rank && (
                <div className="absolute -top-2 -right-2 z-10 w-7 h-7 rounded-full bg-gradient-to-br from-blue-500 via-blue-600 to-cyan-500 flex items-center justify-center text-xs font-black text-white shadow-xl border-3 border-background ring-2 ring-blue-500/30">
                  {opponent.rank}
                </div>
              )}
              <div className="relative w-16 h-16 sm:w-20 sm:h-20 rounded-3xl p-3 sm:p-4 flex items-center justify-center group-hover:scale-110 transition-all duration-500">
                <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-cyan-500/10 rounded-3xl opacity-0 group-hover:opacity-100 transition-opacity blur-xl" />
                <img
                  src={opponent.crestUrl || "/placeholder.svg"}
                  alt={`${opponent.name} crest`}
                  className="w-full h-full object-contain"
                />
              </div>
            </div>
          )}

          {/* Arrow indicator for clickability */}
          <div className="shrink-0 hidden sm:block">
            <ArrowRight className="w-6 h-6 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all duration-300" />
          </div>
        </div>

        <div className="mt-4 pt-4 border-t border-border/30 flex items-center justify-center gap-2 text-sm font-medium text-muted-foreground group-hover:text-primary transition-colors">
          <span>View Fixture Analysis</span>
          <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform sm:hidden" />
        </div>
      </Link>
    </div>
  )
}
