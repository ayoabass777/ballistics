"use client"

import { Layers, Trophy, Goal, SlidersHorizontal } from "lucide-react"
import { FilterPill } from "@/components/FilterPill"
import { ThemeToggle } from "@/components/ThemeToggle"
import type { StreakType } from "@/types/explore-page"

interface FilterBarProps {
  selectedType: "all" | StreakType
  setSelectedType: (type: "all" | StreakType) => void
  resultCount: number
  searchQuery: string
}

export function FilterBar({ selectedType, setSelectedType, resultCount, searchQuery }: FilterBarProps) {
  return (
    <div className="glass-panel p-4 sm:p-6 mb-8 animate-fade-in-up stagger-1">
      <div className="flex flex-col gap-4">
        {/* Header row with title and theme toggle */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-muted/50">
              <SlidersHorizontal className="h-5 w-5 text-muted-foreground" />
            </div>
            <div>
              <h2 className="text-lg font-semibold text-foreground">Filter Streaks</h2>
              <p className="text-xs text-muted-foreground">
                {searchQuery
                  ? `${resultCount} result${resultCount !== 1 ? "s" : ""} for "${searchQuery}"`
                  : `${resultCount} streaks available`}
              </p>
            </div>
          </div>
          <ThemeToggle />
        </div>

        {/* Filter pills - scrollable on mobile */}
        <div className="flex items-center gap-2 overflow-x-auto pb-1 -mb-1 scrollbar-hide">
          <FilterPill
            label="All"
            icon={Layers}
            isActive={selectedType === "all"}
            onClick={() => setSelectedType("all")}
            count={resultCount}
          />
          <FilterPill
            label="Results"
            icon={Trophy}
            isActive={selectedType === "result_streaks"}
            onClick={() => setSelectedType("result_streaks")}
          />
          <FilterPill
            label="Goals"
            icon={Goal}
            isActive={selectedType === "goal_streaks"}
            onClick={() => setSelectedType("goal_streaks")}
          />
        </div>
      </div>
    </div>
  )
}
