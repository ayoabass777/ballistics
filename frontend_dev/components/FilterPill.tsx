"use client"

import type { LucideIcon } from "lucide-react"

interface FilterPillProps {
  label: string
  icon: LucideIcon
  isActive: boolean
  onClick: () => void
  count?: number
}

export function FilterPill({ label, icon: Icon, isActive, onClick, count }: FilterPillProps) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-2 px-4 py-2.5 rounded-xl transition-all duration-300 font-medium text-sm whitespace-nowrap ${
        isActive
          ? "bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/25"
          : "bg-muted/50 text-muted-foreground hover:text-foreground hover:bg-muted border border-transparent hover:border-border"
      }`}
    >
      <Icon className="h-4 w-4" />
      <span>{label}</span>
      {count !== undefined && isActive && (
        <span className="ml-1 px-2 py-0.5 rounded-full bg-white/20 text-xs font-semibold">{count}</span>
      )}
    </button>
  )
}
