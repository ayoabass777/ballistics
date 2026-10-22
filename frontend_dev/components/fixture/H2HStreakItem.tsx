import Image from "next/image"

interface H2HStreakItemProps {
  streak: {
    event: string
    count: number
    total?: number
    teams: "home" | "away" | "both"
  }
  index: number
  homeTeam: {
    logoUrl: string
  }
  awayTeam: {
    logoUrl: string
  }
}

export function H2HStreakItem({ streak, index, homeTeam, awayTeam }: H2HStreakItemProps) {
  return (
    <div
      className="glass-panel p-3 sm:p-4 group hover:border-primary/20 dark:hover:border-primary/30 transition-all duration-300 hover:shadow-lg dark:hover:shadow-primary/5 animate-fade-in-up"
      style={{ animationDelay: `${index * 50}ms` }}
    >
      <div className="relative flex items-center">
        {/* Team Logo(s) - fixed left */}
        <div className="relative shrink-0">
          <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-cyan-500/5 rounded-xl blur-lg opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
          {streak.teams === "both" ? (
            <div className="relative flex -space-x-2">
              <div className="relative w-10 h-10 sm:w-12 sm:h-12 rounded-xl flex items-center justify-center group-hover:scale-105 transition-transform duration-300 z-10">
                <Image
                  src={homeTeam.logoUrl || "/placeholder.svg"}
                  alt="Home"
                  width={32}
                  height={32}
                  className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
                />
              </div>
              <div className="relative w-10 h-10 sm:w-12 sm:h-12 rounded-xl flex items-center justify-center group-hover:scale-105 transition-transform duration-300">
                <Image
                  src={awayTeam.logoUrl || "/placeholder.svg"}
                  alt="Away"
                  width={32}
                  height={32}
                  className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
                />
              </div>
            </div>
          ) : (
            <div className="relative w-10 h-10 sm:w-12 sm:h-12 rounded-xl flex items-center justify-center group-hover:scale-105 transition-transform duration-300">
              <Image
                src={(streak.teams === "home" ? homeTeam.logoUrl : awayTeam.logoUrl) || "/placeholder.svg"}
                alt={streak.teams}
                width={40}
                height={40}
                className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
              />
            </div>
          )}
        </div>

        {/* Event Name - absolutely centered */}
        <div className="absolute left-1/2 -translate-x-1/2 flex items-center justify-center text-center">
          <p className="text-xs sm:text-sm text-foreground font-medium whitespace-nowrap">
            {streak.event}
          </p>
        </div>

        {/* Count - fixed right */}
        <div className="ml-auto text-lg sm:text-xl lg:text-2xl font-black text-foreground font-mono tracking-tight shrink-0 tabular-nums">
          {streak.total ? (
            <span>
              <span className="text-primary">{streak.count}</span>
              <span className="text-muted-foreground">/{streak.total}</span>
            </span>
          ) : (
            <span className="text-primary">{streak.count}</span>
          )}
        </div>
      </div>
    </div>
  )
}
