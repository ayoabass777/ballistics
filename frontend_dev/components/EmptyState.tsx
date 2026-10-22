"use client"

import { Search, TrendingUp, Filter } from "lucide-react"

interface EmptyStateProps {
  type: "search" | "filter" | "no-data"
  searchQuery?: string
  onReset?: () => void
}

/**
 * Empty state component for displaying when no results are found
 */
export function EmptyState({ type, searchQuery, onReset }: EmptyStateProps) {
  const configs = {
    search: {
      icon: Search,
      title: "No streaks found",
      description: searchQuery
        ? `No results found for "${searchQuery}". Try adjusting your search terms.`
        : "Try searching for a team, league, or opponent.",
      action: onReset ? "Clear search" : null,
    },
    filter: {
      icon: Filter,
      title: "No streaks in this league",
      description: "Try selecting a different league or clearing filters to see more results.",
      action: onReset ? "Clear filters" : null,
    },
    "no-data": {
      icon: TrendingUp,
      title: "No active streaks",
      description: "Check back soon for updated streak data across all leagues.",
      action: null,
    },
  }

  const config = configs[type]
  const Icon = config.icon

  return (
    <div className="flex flex-col items-center justify-center py-16 sm:py-20 px-4 text-center animate-fade-in-up">
      <div className="relative mb-6 animate-bounce-slow">
        <div className="absolute inset-0 bg-gradient-to-br from-blue-500/20 to-cyan-500/20 rounded-full blur-2xl animate-pulse" />
        <div className="relative p-6 rounded-2xl bg-gradient-to-br from-blue-500/10 to-cyan-500/10 border border-blue-400/20">
          <Icon className="h-12 w-12 text-blue-400" strokeWidth={1.5} />
        </div>
      </div>

      <h3 className="text-xl sm:text-2xl font-bold text-slate-900 dark:text-white mb-3">{config.title}</h3>

      <p className="text-sm sm:text-base text-slate-600 dark:text-slate-400 max-w-md mb-6">{config.description}</p>

      {config.action && onReset && (
        <button
          onClick={onReset}
          className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-gradient-to-r from-blue-500/20 to-cyan-500/20 border border-blue-400/30 hover:border-blue-400/50 text-slate-900 dark:text-white font-semibold text-sm hover:scale-105 active:scale-95 transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
        >
          {config.action}
        </button>
      )}
    </div>
  )
}
