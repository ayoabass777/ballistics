import Image from "next/image"
import { Home, Plane, Activity } from "lucide-react"
import type { StreakPolarity } from "@/types/fixture-page"

interface StreakItemProps {
  streak: {
    event: string
    count: number
    total?: number
    scope?: string
    polarity?: StreakPolarity
  }
  team: {
    name: string
    logoUrl: string
  }
  index: number
}

const CONTEXT_CONFIG = {
  overall: {
    icon: Activity,
    label: "Overall",
    badge: "bg-zinc-500/10 border-zinc-500/20 text-zinc-600 dark:text-zinc-400",
  },
  home: {
    icon: Home,
    label: "Home",
    badge: "bg-blue-500/10 border-blue-500/20 text-blue-600 dark:text-blue-400",
  },
  away: {
    icon: Plane,
    label: "Away",
    badge: "bg-purple-500/10 border-purple-500/20 text-purple-600 dark:text-purple-400",
  },
}

// Subtle ambient glow colors for polarity - very soft, no harsh blocking
const POLARITY_AMBIENT = {
  positive: {
    glow: "from-emerald-500/5 via-transparent to-transparent",
    countGlow: "drop-shadow-[0_0_8px_rgba(16,185,129,0.3)]",
  },
  negative: {
    glow: "from-red-500/5 via-transparent to-transparent",
    countGlow: "drop-shadow-[0_0_8px_rgba(239,68,68,0.3)]",
  },
  neutral: {
    glow: "from-blue-500/5 via-transparent to-transparent",
    countGlow: "drop-shadow-[0_0_8px_rgba(59,130,246,0.2)]",
  },
}

export function StreakItem({ streak, team, index }: StreakItemProps) {
  const scope = (streak.scope as keyof typeof CONTEXT_CONFIG) || "overall"
  const context = CONTEXT_CONFIG[scope]
  const ContextIcon = context.icon

  const polarity = streak.polarity || "neutral"
  const ambient = POLARITY_AMBIENT[polarity]

  return (
    <div
      className="glass-panel p-3 sm:p-4 group transition-all duration-300 hover:shadow-lg dark:hover:shadow-primary/5 animate-fade-in-up relative overflow-hidden"
      style={{ animationDelay: `${index * 50}ms` }}
    >
      {/* Subtle ambient glow based on polarity - very soft gradient from left */}
      <div className={`absolute inset-0 bg-gradient-to-r ${ambient.glow} pointer-events-none`} />

      <div className="relative flex items-center">
        {/* Team Logo - fixed left */}
        <div className="relative shrink-0">
          <div className="absolute inset-0 bg-gradient-to-br from-blue-500/10 to-cyan-500/5 rounded-xl blur-lg opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
          <div className="relative w-10 h-10 sm:w-12 sm:h-12 rounded-xl flex items-center justify-center group-hover:scale-105 transition-transform duration-300">
            <Image
              src={team.logoUrl || "/placeholder.svg"}
              alt={team.name}
              width={40}
              height={40}
              className="w-8 h-8 sm:w-10 sm:h-10 object-contain"
            />
          </div>
        </div>

        {/* Content - absolutely centered */}
        <div className="absolute left-1/2 -translate-x-1/2 flex flex-col items-center text-center gap-1">
          {/* Context Badge */}
          {streak.scope && (
            <div className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md border ${context.badge} backdrop-blur-sm`}>
              <ContextIcon className="w-3 h-3" />
              <span className="text-[10px] sm:text-xs font-semibold">{context.label}</span>
            </div>
          )}

          {/* Event Name */}
          <p className="text-xs sm:text-sm text-foreground font-medium whitespace-nowrap">
            {streak.event}
          </p>
        </div>

        {/* Right side: Count with subtle glow */}
        <div className="ml-auto shrink-0">
          <div className={`text-lg sm:text-xl lg:text-2xl font-black font-mono tracking-tight tabular-nums text-primary ${ambient.countGlow}`}>
            {streak.total ? (
              <span>
                <span>{streak.count}</span>
                <span className="text-muted-foreground/60">/{streak.total}</span>
              </span>
            ) : (
              <span>{streak.count}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
