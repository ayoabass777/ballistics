"use client"

import { TrendingUp, Globe, Trophy, type LucideIcon } from "lucide-react"

interface StatItem {
  icon: LucideIcon
  value: string
  label: string
}

const stats: StatItem[] = [
  { icon: TrendingUp, value: "500+", label: "Active Streaks" },
  { icon: Globe, value: "10", label: "Leagues" },
  { icon: Trophy, value: "250+", label: "Teams Tracked" },
]

export function QuickStats() {
  return (
    <div className="grid grid-cols-3 gap-3 sm:gap-4 md:gap-6 mb-10 sm:mb-12">
      {stats.map((stat, index) => (
        <div
          key={index}
          className="glass-panel flex flex-col sm:flex-row items-center gap-2 sm:gap-3 px-3 sm:px-5 py-3 sm:py-4 rounded-xl hover:border-blue-400/30 transition-all duration-300 hover:scale-105 hover:shadow-xl hover:shadow-blue-500/10 cursor-default"
          style={{ animationDelay: `${index * 100}ms` }}
        >
          <div className="p-2 rounded-xl bg-gradient-to-br from-blue-500/20 to-cyan-500/10">
            <stat.icon className="h-4 w-4 sm:h-5 sm:w-5 text-blue-400" />
          </div>
          <div className="text-center sm:text-left">
            <p className="text-lg sm:text-xl md:text-2xl font-bold text-slate-900 dark:text-white">{stat.value}</p>
            <p className="text-xs text-muted-foreground">{stat.label}</p>
          </div>
        </div>
      ))}
    </div>
  )
}
