"use client"

import { X } from "lucide-react"

interface FilterChip {
  label: string
  value: string
  onRemove: () => void
}

interface FilterChipsProps {
  filters: FilterChip[]
  onClearAll?: () => void
}

/**
 * Component for displaying active filters as removable chips
 * Helps users understand what filters are applied
 */
export function FilterChips({ filters, onClearAll }: FilterChipsProps) {
  if (filters.length === 0) return null

  return (
    <div className="flex items-center gap-2 flex-wrap animate-fade-in">
      <span className="text-xs font-semibold text-slate-600 dark:text-slate-400 uppercase tracking-wider">
        Active Filters:
      </span>

      {filters.map((filter, index) => (
        <div
          key={index}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-blue-500/10 border border-blue-400/30 text-slate-900 dark:text-white text-sm animate-scale-in group hover:bg-blue-500/20 transition-colors"
          style={{ animationDelay: `${index * 50}ms` }}
        >
          <span className="font-medium">{filter.label}</span>
          <button
            onClick={filter.onRemove}
            className="p-0.5 rounded-full hover:bg-blue-500/30 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400"
            aria-label={`Remove ${filter.label} filter`}
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      ))}

      {filters.length > 1 && onClearAll && (
        <button
          onClick={onClearAll}
          className="text-xs font-semibold text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white underline underline-offset-2 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400 rounded px-2 py-1"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
