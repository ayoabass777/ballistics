import { Users } from "lucide-react"
import Image from "next/image"

interface H2HOverviewProps {
  h2hStats: {
    homeWins: number
    draws: number
    awayWins: number
    totalMatches: number
  }
  homeTeam: {
    name: string
    logoUrl: string
  }
  awayTeam: {
    name: string
    logoUrl: string
  }
}

type StatCardProps = {
  value: number
  label: string
  color: "blue" | "zinc" | "purple"
  ariaLabel: string
  logo?: string
  logoAlt?: string
}

function StatCard({ value, label, color, ariaLabel, logo, logoAlt }: StatCardProps) {
  const colorStyles = {
    blue: {
      hover: "hover:border-blue-500/40 hover:bg-gradient-to-br hover:from-blue-500/10 hover:to-cyan-500/5",
      overlay: "bg-gradient-to-br from-blue-500/5 to-transparent",
      text: "text-blue-500 dark:text-blue-400",
    },
    zinc: {
      hover: "hover:border-zinc-500/40 hover:bg-zinc-500/5",
      overlay: "",
      text: "text-zinc-500 dark:text-zinc-400",
    },
    purple: {
      hover: "hover:border-purple-500/40 hover:bg-gradient-to-br hover:from-purple-500/10 hover:to-pink-500/5",
      overlay: "bg-gradient-to-br from-purple-500/5 to-transparent",
      text: "text-purple-500 dark:text-purple-400",
    },
  }

  const styles = colorStyles[color]

  return (
    <div
      className={`flex-1 text-center p-3 sm:p-5 rounded-2xl bg-transparent border border-border/30 ${styles.hover} transition-all duration-300 group relative overflow-hidden`}
      role="region"
      aria-label={ariaLabel}
    >
      {styles.overlay && (
        <div className={`absolute inset-0 ${styles.overlay} opacity-0 group-hover:opacity-100 transition-opacity duration-300`} />
      )}
      <div className="relative">
        <div className="flex justify-center mb-3 sm:mb-4">
          <div className={`w-10 h-10 sm:w-14 sm:h-14 lg:w-16 lg:h-16 rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform duration-300 ${!logo ? 'bg-zinc-500/10 border border-zinc-500/20' : ''}`}>
            {logo ? (
              <Image
                src={logo}
                alt={logoAlt || ""}
                width={48}
                height={48}
                className="w-8 h-8 sm:w-12 sm:h-12 object-contain"
              />
            ) : (
              <span className="text-lg sm:text-xl lg:text-2xl font-black text-zinc-400">=</span>
            )}
          </div>
        </div>
        <div className={`text-3xl sm:text-4xl lg:text-5xl font-black ${styles.text} mb-1 font-mono tracking-tight tabular-nums`}>
          {value}
        </div>
        <div className="text-[10px] sm:text-xs text-muted-foreground font-semibold uppercase tracking-wide">
          {label}
        </div>
      </div>
    </div>
  )
}

export function H2HOverview({ h2hStats, homeTeam, awayTeam }: H2HOverviewProps) {
  const total = h2hStats.homeWins + h2hStats.draws + h2hStats.awayWins
  const homePercent = total > 0 ? (h2hStats.homeWins / total) * 100 : 33.33
  const drawPercent = total > 0 ? (h2hStats.draws / total) * 100 : 33.34
  const awayPercent = total > 0 ? (h2hStats.awayWins / total) * 100 : 33.33

  return (
    <section aria-labelledby="h2h-title">
      <div className="flex flex-col items-center text-center gap-3 mb-5">
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border border-blue-500/20 flex items-center justify-center">
          <Users className="h-5 w-5 text-blue-400" aria-hidden="true" />
        </div>
        <div>
          <h3 id="h2h-title" className="text-xl sm:text-2xl font-bold text-foreground">
            Head-to-Head
          </h3>
          <p className="text-sm text-muted-foreground">Historical patterns between these teams</p>
        </div>
      </div>

      <div className="glass-panel-elevated p-4 sm:p-6 animate-fade-in-up">
        {/* Progress Bar */}
        <div className="mb-6">
          <div className="flex h-3 sm:h-4 rounded-full overflow-hidden bg-muted/30">
            <div
              className="bg-gradient-to-r from-blue-600 to-blue-400 transition-all duration-700 ease-out"
              style={{ width: `${homePercent}%` }}
              title={`${homeTeam.name}: ${h2hStats.homeWins} wins`}
            />
            <div
              className="bg-gradient-to-r from-zinc-500 to-zinc-400 transition-all duration-700 ease-out"
              style={{ width: `${drawPercent}%` }}
              title={`Draws: ${h2hStats.draws}`}
            />
            <div
              className="bg-gradient-to-r from-purple-400 to-purple-600 transition-all duration-700 ease-out"
              style={{ width: `${awayPercent}%` }}
              title={`${awayTeam.name}: ${h2hStats.awayWins} wins`}
            />
          </div>
        </div>

        {/* Stat Cards */}
        <div className="flex items-stretch gap-2 sm:gap-3">
          <StatCard
            value={h2hStats.homeWins}
            label="Wins"
            color="blue"
            ariaLabel={`${homeTeam.name} wins: ${h2hStats.homeWins}`}
            logo={homeTeam.logoUrl}
            logoAlt={homeTeam.name}
          />
          <StatCard
            value={h2hStats.draws}
            label="Draws"
            color="zinc"
            ariaLabel={`Draws: ${h2hStats.draws}`}
          />
          <StatCard
            value={h2hStats.awayWins}
            label="Wins"
            color="purple"
            ariaLabel={`${awayTeam.name} wins: ${h2hStats.awayWins}`}
            logo={awayTeam.logoUrl}
            logoAlt={awayTeam.name}
          />
        </div>

        <div className="text-center text-xs text-muted-foreground mt-5 pt-4 border-t border-border/30 font-medium">
          Last {h2hStats.totalMatches} meetings
        </div>
      </div>
    </section>
  )
}
