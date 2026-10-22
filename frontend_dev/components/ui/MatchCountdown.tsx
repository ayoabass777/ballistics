"use client"

import { Clock } from "lucide-react"
import { cn } from "@/lib/utils"
import { formatMatchTime } from "@/lib/formatMatchTime"

interface MatchCountdownProps {
  matchDate: string
  className?: string
}

export function MatchCountdown({ matchDate, className }: MatchCountdownProps) {
  const { label, isUrgent, isPast } = formatMatchTime(matchDate)

  if (isPast) return null

  return (
    <div
      className={cn(
        "flex items-center gap-1.5 text-xs",
        isUrgent
          ? "text-orange-600 dark:text-orange-400 font-medium"
          : "text-muted-foreground",
        className
      )}
    >
      <Clock className={cn("w-3 h-3", isUrgent && "animate-pulse")} />
      <span>{label}</span>
    </div>
  )
}
