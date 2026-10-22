import { Calendar, MapPin } from "lucide-react"
import Image from "next/image"
import Link from "next/link"
import { FormDots } from "@/components/ui/FormDots"

interface FixtureMatchupHeroProps {
  fixture: {
    league: {
      name: string
      logoUrl: string
      matchday?: number
    }
    kickoff: {
      iso: string
    }
    venue: {
      name: string
      city: string
      imageUrl?: string
    }
    homeTeam: {
      name: string
      slug: string
      logoUrl: string
      recentForm?: Array<"W" | "D" | "L">
      formByContext?: {
        overall: Array<"W" | "D" | "L">
        home: Array<"W" | "D" | "L">
        away: Array<"W" | "D" | "L">
      }
    }
    awayTeam: {
      name: string
      slug: string
      logoUrl: string
      recentForm?: Array<"W" | "D" | "L">
      formByContext?: {
        overall: Array<"W" | "D" | "L">
        home: Array<"W" | "D" | "L">
        away: Array<"W" | "D" | "L">
      }
    }
    homeTeamHref?: string
    awayTeamHref?: string
  }
}

export function FixtureMatchupHero({ fixture }: FixtureMatchupHeroProps) {
  // Align fixture-page dots with streak-card dots: both use overall recent form.
  const homeTeamForm = fixture.homeTeam.recentForm || []
  const awayTeamForm = fixture.awayTeam.recentForm || []

  return (
    <section
      className="relative glass-panel-elevated overflow-hidden p-4 sm:p-6 lg:p-8 animate-fade-in"
      aria-label="Fixture matchup overview"
    >
      {/* Stadium background */}
      {fixture.venue.imageUrl && (
        <>
          <div className="absolute inset-0 z-0">
            <Image
              src={fixture.venue.imageUrl || "/placeholder.svg"}
              alt={`${fixture.venue.name} stadium`}
              fill
              className="object-cover"
              priority
            />
          </div>
          {/* Overlay - different opacity for light/dark modes */}
          <div className="absolute inset-0 z-0 bg-gradient-to-b from-background/80 via-background/70 to-background/80 dark:from-background/90 dark:via-background/85 dark:to-background/90" />
          <div className="absolute inset-0 z-0 backdrop-blur-[2px]" />
        </>
      )}

      <div className="relative z-10">
        {/* League badge */}
        <div className="flex items-center justify-center gap-2 mb-6 sm:mb-8">
          <div className="flex items-center gap-2.5 px-4 py-2 rounded-full bg-gradient-to-r from-blue-500/10 via-cyan-500/10 to-blue-500/10 border border-blue-500/20 backdrop-blur-sm shadow-sm transition-all duration-300 hover:shadow-md hover:border-blue-500/30">
            <Image
              src={fixture.league.logoUrl || "/placeholder.svg"}
              alt={fixture.league.name}
              width={20}
              height={20}
              className="object-contain"
            />
            <span className="text-sm font-bold text-foreground">{fixture.league.name}</span>
            {fixture.league.matchday && (
              <>
                <span className="text-xs text-muted-foreground">•</span>
                <span className="text-xs text-muted-foreground font-medium">MD {fixture.league.matchday}</span>
              </>
            )}
          </div>
        </div>

        {/* Matchup grid */}
        <div className="relative mb-6 sm:mb-8">
          <div className="grid grid-cols-[1fr_auto_1fr] gap-4 sm:gap-8 lg:gap-12 items-center">
            {/* Home team */}
            <Link
              href={fixture.homeTeamHref || `/team/${fixture.homeTeam.slug}`}
              className="flex flex-col items-center gap-2 sm:gap-3 group"
              aria-label={`View ${fixture.homeTeam.name} team stats`}
            >
              <div className="relative">
                {/* Glow effect on hover */}
                <div className="absolute inset-0 bg-blue-500/20 rounded-full blur-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
                <div className="relative w-20 h-20 sm:w-24 sm:h-24 lg:w-28 lg:h-28 rounded-2xl p-3 sm:p-4 flex items-center justify-center group-hover:scale-110 transition-all duration-300 ease-out">
                  <Image
                    src={fixture.homeTeam.logoUrl || "/placeholder.svg"}
                    alt={fixture.homeTeam.name}
                    width={96}
                    height={96}
                    className="object-contain drop-shadow-lg"
                  />
                </div>
              </div>
              <h2 className="text-sm sm:text-base lg:text-lg font-bold text-foreground group-hover:text-blue-500 dark:group-hover:text-blue-400 transition-colors duration-300 text-center leading-tight">
                {fixture.homeTeam.name}
              </h2>
              {/* Home form dots (same source as streak cards) */}
              {homeTeamForm.length > 0 && (
                <FormDots form={homeTeamForm.slice(0, 5)} size="sm" />
              )}
            </Link>

            {/* VS badge with date/time */}
            <div className="flex flex-col items-center justify-center gap-3 sm:gap-4 shrink-0">
              <div className="relative group">
                {/* Animated glow */}
                <div className="absolute inset-0 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-2xl blur-lg opacity-50 group-hover:opacity-70 transition-opacity duration-300 animate-pulse-slow" />
                <div className="relative w-14 h-14 sm:w-18 sm:h-18 lg:w-20 lg:h-20 rounded-2xl bg-gradient-to-br from-blue-500 to-cyan-500 flex items-center justify-center shadow-2xl group-hover:scale-105 transition-transform duration-300">
                  <span className="text-white font-black text-lg sm:text-xl lg:text-2xl tracking-tight">VS</span>
                </div>
              </div>
              <div className="flex flex-col items-center gap-1 text-center" role="group" aria-label="Match kickoff time">
                <div className="flex items-center gap-1.5 text-xs sm:text-sm text-muted-foreground">
                  <Calendar className="h-3.5 w-3.5" aria-hidden="true" />
                  <span className="font-medium">
                    {new Date(fixture.kickoff.iso).toLocaleDateString("en-US", { month: "short", day: "numeric" })}
                  </span>
                </div>
                <span className="text-sm sm:text-base font-mono font-bold text-foreground">
                  {new Date(fixture.kickoff.iso).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false })}
                </span>
              </div>
            </div>

            {/* Away team */}
            <Link
              href={fixture.awayTeamHref || `/team/${fixture.awayTeam.slug}`}
              className="flex flex-col items-center gap-2 sm:gap-3 group"
              aria-label={`View ${fixture.awayTeam.name} team stats`}
            >
              <div className="relative">
                {/* Glow effect on hover */}
                <div className="absolute inset-0 bg-purple-500/20 rounded-full blur-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500" />
                <div className="relative w-20 h-20 sm:w-24 sm:h-24 lg:w-28 lg:h-28 rounded-2xl p-3 sm:p-4 flex items-center justify-center group-hover:scale-110 transition-all duration-300 ease-out">
                  <Image
                    src={fixture.awayTeam.logoUrl || "/placeholder.svg"}
                    alt={fixture.awayTeam.name}
                    width={96}
                    height={96}
                    className="object-contain drop-shadow-lg"
                  />
                </div>
              </div>
              <h2 className="text-sm sm:text-base lg:text-lg font-bold text-foreground group-hover:text-purple-500 dark:group-hover:text-purple-400 transition-colors duration-300 text-center leading-tight">
                {fixture.awayTeam.name}
              </h2>
              {/* Away form dots (same source as streak cards) */}
              {awayTeamForm.length > 0 && (
                <FormDots form={awayTeamForm.slice(0, 5)} size="sm" />
              )}
            </Link>
          </div>
        </div>

        {/* Venue info */}
        <div className="flex flex-wrap items-center justify-center gap-3 text-sm text-muted-foreground pt-5 sm:pt-6 border-t border-border/30">
          <div
            className="flex items-center gap-2 px-4 py-2 rounded-full bg-muted/30 dark:bg-muted/20 backdrop-blur-sm border border-border/50 transition-all duration-300 hover:bg-muted/40 dark:hover:bg-muted/30"
            role="group"
            aria-label="Match venue"
          >
            <MapPin className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            <span className="font-medium">
              {fixture.venue.name}, {fixture.venue.city}
            </span>
          </div>
        </div>
      </div>
    </section>
  )
}
