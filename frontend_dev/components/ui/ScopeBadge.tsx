"use client"

import { cn } from "@/lib/utils"
import { Home, Plane, Activity } from "lucide-react"

type StreakScope = "overall" | "home" | "away"

interface ScopeBadgeProps {
  scope: StreakScope
  showIcon?: boolean
  className?: string
}

const scopeConfig: Record<
  StreakScope,
  {
    label: string
    icon: typeof Activity
    containerClass: string
    iconClass: string
    textClass: string
  }
> = {
  overall: {
    label: "Overall",
    icon: Activity,
    containerClass:
      "bg-slate-100 dark:bg-slate-800 border-slate-200 dark:border-slate-700",
    iconClass: "text-slate-500 dark:text-slate-400",
    textClass: "text-slate-600 dark:text-slate-300",
  },
  home: {
    label: "Home",
    icon: Home,
    containerClass:
      "bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200 dark:border-emerald-800/50",
    iconClass: "text-emerald-600 dark:text-emerald-400",
    textClass: "text-emerald-700 dark:text-emerald-300",
  },
  away: {
    label: "Away",
    icon: Plane,
    containerClass:
      "bg-amber-50 dark:bg-amber-950/40 border-amber-200 dark:border-amber-800/50",
    iconClass: "text-amber-600 dark:text-amber-400",
    textClass: "text-amber-700 dark:text-amber-300",
  },
}

export function ScopeBadge({
  scope,
  showIcon = true,
  className,
}: ScopeBadgeProps) {
  const config = scopeConfig[scope]
  const Icon = config.icon

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-xs font-semibold uppercase tracking-wide",
        config.containerClass,
        className
      )}
    >
      {showIcon && <Icon className={cn("w-3 h-3", config.iconClass)} />}
      <span className={config.textClass}>{config.label}</span>
    </span>
  )
}
