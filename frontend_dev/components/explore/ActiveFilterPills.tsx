"use client"

import { X } from "lucide-react"
import { cn } from "@/lib/utils"

interface ActiveFilter {
  key: string
  label: string
  onRemove: () => void
}

interface ActiveFilterPillsProps {
  filters: ActiveFilter[]
  onClearAll: () => void
  className?: string
}

export function ActiveFilterPills({
  filters,
  onClearAll,
  className,
}: ActiveFilterPillsProps) {
  if (filters.length === 0) return null

  return (
    <div className={cn("flex items-center gap-2 flex-wrap", className)}>
      <span className="text-sm text-muted-foreground">Active:</span>

      {filters.map((filter) => (
        <button
          key={filter.key}
          onClick={filter.onRemove}
          className={cn(
            "inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium",
            "bg-primary/10 text-primary hover:bg-primary/20 transition-colors",
            "border border-primary/20"
          )}
        >
          {filter.label}
          <X className="w-3 h-3" />
        </button>
      ))}

      {filters.length > 1 && (
        <button
          onClick={onClearAll}
          className="text-xs text-muted-foreground hover:text-foreground underline underline-offset-2 transition-colors"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
