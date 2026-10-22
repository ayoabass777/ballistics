"use client"

import { Home, Plane, Activity } from "lucide-react"
import type { StreakCardVM } from "@/types/team-page"

/**
 * StreakMiniCard - Compact streak display for team page
 *
 * Design principles (from architecture):
 * - Shows streak headline prominently
 * - Scope badge (overall/home/away) for context
 * - Streak length as the key number
 * - Polarity determines subtle ambient visual treatment:
 *   - Positive: soft emerald glow
 *   - Negative: soft red glow
 *   - Neutral: soft blue glow
 */

const SCOPE_CONFIG = {
  overall: {
    icon: Activity,
    label: "Overall",
    badgeClass: "bg-secondary/60 border-border/50 text-muted-foreground",
  },
  home: {
    icon: Home,
    label: "Home",
    badgeClass: "bg-blue-500/10 border-blue-500/20 text-blue-600 dark:text-blue-400",
  },
  away: {
    icon: Plane,
    label: "Away",
    badgeClass: "bg-purple-500/10 border-purple-500/20 text-purple-600 dark:text-purple-400",
  },
}

// Subtle ambient glow colors for polarity - very soft, no harsh blocking
const POLARITY_AMBIENT = {
  positive: {
    glow: "from-emerald-500/5 via-transparent to-transparent",
    lengthGlow: "drop-shadow-[0_0_10px_rgba(16,185,129,0.35)]",
  },
  negative: {
    glow: "from-red-500/5 via-transparent to-transparent",
    lengthGlow: "drop-shadow-[0_0_10px_rgba(239,68,68,0.35)]",
  },
  neutral: {
    glow: "from-blue-500/5 via-transparent to-transparent",
    lengthGlow: "drop-shadow-[0_0_10px_rgba(59,130,246,0.25)]",
  },
}

export function StreakMiniCard({ streak }: { streak: StreakCardVM }) {
  const scope = (streak.scope || "overall") as keyof typeof SCOPE_CONFIG
  const scopeConfig = SCOPE_CONFIG[scope]
  const ScopeIcon = scopeConfig.icon

  const polarity = streak.polarity || "neutral"
  const ambient = POLARITY_AMBIENT[polarity]

  return (
    <div className="glass-panel p-4 sm:p-5 relative overflow-hidden transition-all duration-300 hover:shadow-lg dark:hover:shadow-primary/5">
      {/* Subtle ambient glow based on polarity - very soft gradient from left */}
      <div className={`absolute inset-0 bg-gradient-to-r ${ambient.glow} pointer-events-none`} />

      <div className="relative space-y-3">
        {/* Header: Scope badge */}
        <div className="flex items-center justify-between">
          <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg border ${scopeConfig.badgeClass}`}>
            <ScopeIcon className="w-3 h-3" />
            <span className="text-xs font-medium">{scopeConfig.label}</span>
          </div>
        </div>

        {/* Main Content: headline and streak number */}
        <div className="flex items-center gap-3">
          {/* Streak headline */}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-semibold text-foreground leading-tight">
              {streak.headline}
            </p>
          </div>

          {/* Streak length with subtle glow */}
          <div className={`text-2xl font-black text-primary shrink-0 ${ambient.lengthGlow}`}>
            {streak.length}
          </div>
        </div>
      </div>
    </div>
  )
}
