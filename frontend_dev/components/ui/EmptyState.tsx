"use client"

import { Search, RefreshCw } from "lucide-react"
import { cn } from "@/lib/utils"

interface Suggestion {
  label: string
  action: () => void
}

interface EmptyStateProps {
  title?: string
  description: string
  suggestions?: Suggestion[]
  onClearFilters?: () => void
  className?: string
}

export function EmptyState({
  title = "No streaks found",
  description,
  suggestions = [],
  onClearFilters,
  className,
}: EmptyStateProps) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center py-16 px-4 text-center",
        className
      )}
    >
      <div className="w-14 h-14 rounded-2xl bg-muted/50 flex items-center justify-center mb-5">
        <Search className="w-7 h-7 text-muted-foreground" />
      </div>

      <h3 className="text-xl font-semibold mb-2 text-foreground">{title}</h3>
      <p className="text-muted-foreground max-w-md mb-6">{description}</p>

      {(suggestions.length > 0 || onClearFilters) && (
        <div className="flex flex-col items-center gap-3">
          <p className="text-sm text-muted-foreground">Try:</p>
          <div className="flex flex-wrap justify-center gap-2">
            {suggestions.map((suggestion, index) => (
              <button
                key={index}
                onClick={suggestion.action}
                className="inline-flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-medium bg-secondary/60 dark:bg-secondary/40 text-foreground hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-primary/30 transition-all duration-200"
              >
                {suggestion.label}
              </button>
            ))}
          </div>
          {onClearFilters && (
            <button
              onClick={onClearFilters}
              className="inline-flex items-center gap-2 px-4 py-2 mt-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Clear all filters
            </button>
          )}
        </div>
      )}
    </div>
  )
}
