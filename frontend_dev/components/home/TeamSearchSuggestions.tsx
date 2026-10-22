"use client"

import type { TeamSearchSuggestion } from "@/types/search"

interface TeamSearchSuggestionsProps {
  query: string
  suggestions: TeamSearchSuggestion[]
  loading: boolean
  error: string | null
  visible: boolean
  onSelect: (suggestion: TeamSearchSuggestion) => void
}

export function TeamSearchSuggestions({
  query,
  suggestions,
  loading,
  error,
  visible,
  onSelect,
}: TeamSearchSuggestionsProps) {
  if (!visible) return null

  const trimmedQuery = query.trim()

  return (
    <div className="absolute top-full left-0 right-0 mt-2 rounded-xl border border-border/60 bg-popover/98 backdrop-blur-xl shadow-xl shadow-black/10 dark:shadow-black/30 z-[110] overflow-hidden">
      {loading && (
        <div className="px-3 py-3 text-xs text-muted-foreground">Searching teams...</div>
      )}

      {!loading && error && (
        <div className="px-3 py-3 text-xs text-amber-600 dark:text-amber-400">{error}</div>
      )}

      {!loading && !error && trimmedQuery.length < 2 && (
        <div className="px-3 py-3 text-xs text-muted-foreground">Type at least 2 letters to search teams</div>
      )}

      {!loading && !error && trimmedQuery.length >= 2 && suggestions.length === 0 && (
        <div className="px-3 py-3 text-xs text-muted-foreground">No team matches found</div>
      )}

      {!loading && suggestions.length > 0 && (
        <div className="py-1">
          {suggestions.map((suggestion) => (
            <button
              key={suggestion.teamId}
              type="button"
              onClick={() => onSelect(suggestion)}
              className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-secondary/70 dark:hover:bg-secondary/50 transition-colors"
            >
              <span className="flex h-7 w-7 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5 shrink-0">
                {suggestion.logoUrl ? (
                  <img src={suggestion.logoUrl} alt="" className="h-6 w-6 object-contain" />
                ) : (
                  <span className="text-[10px] font-bold text-slate-500">FC</span>
                )}
              </span>
              <div className="min-w-0">
                <div className="text-sm font-medium text-foreground truncate">{suggestion.name}</div>
                <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground truncate">
                  {suggestion.countryLogoUrl && (
                    <span className="flex h-3.5 w-3.5 items-center justify-center rounded-sm bg-white/95 ring-1 ring-black/5 shrink-0">
                      <img src={suggestion.countryLogoUrl} alt="" className="h-3.5 w-3.5 rounded-sm object-cover" />
                    </span>
                  )}
                  <span className="truncate">{suggestion.leagueName || "Team"}</span>
                </div>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
