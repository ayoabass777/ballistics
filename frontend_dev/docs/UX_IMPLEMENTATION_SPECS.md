# UX Implementation Specs

> Detailed implementation specifications for priority UX improvements.

**Created:** January 2025
**Status:** Ready for Implementation

---

## Table of Contents

1. [Scope Badge Prominence](#1-scope-badge-prominence)
2. [Match Countdown](#2-match-countdown)
3. [Form Dots Visualization](#3-form-dots-visualization)
4. [Sticky Filter Bar](#4-sticky-filter-bar)
5. [League Quick-Chips](#5-league-quick-chips)
6. [Opponent Logo Display](#6-opponent-logo-display)
7. [Empty State with Suggestions](#7-empty-state-with-suggestions)
8. [Active Filter Summary](#8-active-filter-summary)

---

## 1. Scope Badge Prominence

### Problem
The scope (overall/home/away) is critical context for understanding a streak's significance. A 5-match home win streak is very different from 5 overall wins. Currently, scope may not be visually prominent enough.

### Solution
Add a colored badge that clearly indicates scope on every streak card.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  [Team Logo]  Manchester City                   │
│               ┌─────────┐                       │
│               │  HOME   │  ← Colored badge      │
│               └─────────┘                       │
│  5 Consecutive Wins                             │
│  ● ● ● ● ●   vs Newcastle • Tomorrow           │
└─────────────────────────────────────────────────┘
```

### Badge Variants

| Scope | Background | Text | Border |
|-------|------------|------|--------|
| `overall` | `bg-slate-100 dark:bg-slate-800` | `text-slate-600 dark:text-slate-300` | none |
| `home` | `bg-emerald-100 dark:bg-emerald-900/30` | `text-emerald-700 dark:text-emerald-400` | none |
| `away` | `bg-amber-100 dark:bg-amber-900/30` | `text-amber-700 dark:text-amber-400` | none |

### Component Implementation

```typescript
// components/ui/ScopeBadge.tsx
import { cn } from "@/lib/utils"
import { StreakScope } from "@/types"

interface ScopeBadgeProps {
  scope: StreakScope
  className?: string
}

const scopeStyles: Record<StreakScope, string> = {
  overall: "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300",
  home: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400",
  away: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400",
}

const scopeLabels: Record<StreakScope, string> = {
  overall: "Overall",
  home: "Home",
  away: "Away",
}

export function ScopeBadge({ scope, className }: ScopeBadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase tracking-wide",
        scopeStyles[scope],
        className
      )}
    >
      {scopeLabels[scope]}
    </span>
  )
}
```

### Usage in StreakCard

```typescript
// In StreakCard component
<div className="flex items-center justify-between">
  <div className="flex items-center gap-3">
    <img src={team_logo_url} alt={team_name} className="w-10 h-10" />
    <div>
      <h3 className="font-semibold">{team_name}</h3>
      <ScopeBadge scope={scope} className="mt-1" />
    </div>
  </div>
</div>
```

### Accessibility
- Badge text is readable (not just color-coded)
- Sufficient color contrast in both light/dark modes
- Screen reader: "5 consecutive wins, home matches"

---

## 2. Match Countdown

### Problem
Showing "2025-01-25T15:00:00Z" or even "Jan 25" doesn't create urgency. Bettors need to know how soon the streak is "at risk."

### Solution
Display relative time: "Today 3pm", "Tomorrow", "In 2 days", "Saturday".

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  5 Consecutive Wins                             │
│                                                 │
│  vs Newcastle United                            │
│  ⏱ Tomorrow, 3:00 PM    ← Countdown format     │
└─────────────────────────────────────────────────┘
```

### Countdown Logic

| Time Until Match | Display |
|------------------|---------|
| < 2 hours | "Starting soon" (with pulse animation) |
| Today | "Today, 3:00 PM" |
| Tomorrow | "Tomorrow, 3:00 PM" |
| 2-6 days | "In 2 days" / "Saturday" |
| 7+ days | "Jan 25" |
| Past | "Played" or hide |

### Component Implementation

```typescript
// lib/formatMatchTime.ts
import { formatDistanceToNow, isToday, isTomorrow, format, differenceInHours, differenceInDays, isPast } from "date-fns"

export interface MatchTimeDisplay {
  label: string
  isUrgent: boolean  // For styling (pulse animation)
  isPast: boolean
}

export function formatMatchTime(dateString: string): MatchTimeDisplay {
  const matchDate = new Date(dateString)
  const now = new Date()

  if (isPast(matchDate)) {
    return { label: "Played", isUrgent: false, isPast: true }
  }

  const hoursUntil = differenceInHours(matchDate, now)
  const daysUntil = differenceInDays(matchDate, now)
  const timeFormat = format(matchDate, "h:mm a") // "3:00 PM"

  if (hoursUntil < 2) {
    return { label: "Starting soon", isUrgent: true, isPast: false }
  }

  if (isToday(matchDate)) {
    return { label: `Today, ${timeFormat}`, isUrgent: true, isPast: false }
  }

  if (isTomorrow(matchDate)) {
    return { label: `Tomorrow, ${timeFormat}`, isUrgent: false, isPast: false }
  }

  if (daysUntil <= 6) {
    const dayName = format(matchDate, "EEEE") // "Saturday"
    return { label: `${dayName}, ${timeFormat}`, isUrgent: false, isPast: false }
  }

  // More than a week away
  return { label: format(matchDate, "MMM d"), isUrgent: false, isPast: false }
}
```

### Component

```typescript
// components/ui/MatchCountdown.tsx
import { Clock } from "lucide-react"
import { cn } from "@/lib/utils"
import { formatMatchTime } from "@/lib/formatMatchTime"

interface MatchCountdownProps {
  matchDate: string
  className?: string
}

export function MatchCountdown({ matchDate, className }: MatchCountdownProps) {
  const { label, isUrgent, isPast } = formatMatchTime(matchDate)

  if (isPast) return null // Or show "Played" badge

  return (
    <div
      className={cn(
        "flex items-center gap-1.5 text-sm",
        isUrgent ? "text-orange-600 dark:text-orange-400" : "text-muted-foreground",
        className
      )}
    >
      <Clock className={cn("w-3.5 h-3.5", isUrgent && "animate-pulse")} />
      <span>{label}</span>
    </div>
  )
}
```

### Usage

```typescript
<MatchCountdown matchDate={next_match_date} />
```

---

## 3. Form Dots Visualization

### Problem
Reading "W W D W L" requires cognitive processing. Visual patterns are recognized faster.

### Solution
Colored dots that show form at a glance.

### Design Spec

```
Recent Form:
● ● ● ○ ●     ← Green=Win, Gray=Draw, Red=Loss
W W W D W     ← Optional: show letters on hover/below
```

### Color Scheme

| Result | Color (Light) | Color (Dark) |
|--------|---------------|--------------|
| Win (W) | `bg-emerald-500` | `bg-emerald-400` |
| Draw (D) | `bg-slate-300` | `bg-slate-500` |
| Loss (L) | `bg-red-500` | `bg-red-400` |

### Component Implementation

```typescript
// components/ui/FormDots.tsx
import { cn } from "@/lib/utils"
import { MatchResult } from "@/types"

interface FormDotsProps {
  form: MatchResult[]
  showLabels?: boolean
  size?: "sm" | "md" | "lg"
  className?: string
}

const resultColors: Record<MatchResult, string> = {
  W: "bg-emerald-500 dark:bg-emerald-400",
  D: "bg-slate-300 dark:bg-slate-500",
  L: "bg-red-500 dark:bg-red-400",
}

const resultLabels: Record<MatchResult, string> = {
  W: "Win",
  D: "Draw",
  L: "Loss",
}

const sizeClasses = {
  sm: "w-2 h-2",
  md: "w-2.5 h-2.5",
  lg: "w-3 h-3",
}

export function FormDots({ form, showLabels = false, size = "md", className }: FormDotsProps) {
  return (
    <div className={cn("flex items-center gap-1", className)}>
      {form.map((result, index) => (
        <div
          key={index}
          className="flex flex-col items-center gap-0.5"
          title={resultLabels[result]}
        >
          <span
            className={cn(
              "rounded-full",
              sizeClasses[size],
              resultColors[result]
            )}
            aria-label={resultLabels[result]}
          />
          {showLabels && (
            <span className="text-[10px] text-muted-foreground">{result}</span>
          )}
        </div>
      ))}
    </div>
  )
}
```

### Accessibility Enhancement

```typescript
// Add screen reader description
export function FormDots({ form, ...props }: FormDotsProps) {
  const description = form
    .map((r) => resultLabels[r])
    .join(", ")

  return (
    <div role="img" aria-label={`Recent form: ${description}`}>
      {/* dots */}
    </div>
  )
}
```

### Usage

```typescript
// Basic
<FormDots form={["W", "W", "D", "W", "L"]} />

// With labels below
<FormDots form={recent_form} showLabels size="lg" />

// Small variant for compact cards
<FormDots form={recent_form} size="sm" />
```

---

## 4. Sticky Filter Bar

### Problem
On the Explore page with infinite scroll, users must scroll back to top to change filters.

### Solution
Make the filter bar sticky at the top when scrolling.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  [Logo]  Ballistics           [Theme] [Menu]   │  ← Main header
├─────────────────────────────────────────────────┤
│  [All] [Results] [Goals]  |  [League ▼] [🔍]   │  ← Sticky filter bar
├─────────────────────────────────────────────────┤
│                                                 │
│  [Streak Card]                                  │
│  [Streak Card]                                  │
│  [Streak Card]                                  │  ← Scrollable content
│  ...                                            │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Implementation

```typescript
// components/explore/StickyFilterBar.tsx
"use client"

import { cn } from "@/lib/utils"
import { useEffect, useState } from "react"

interface StickyFilterBarProps {
  children: React.ReactNode
  className?: string
}

export function StickyFilterBar({ children, className }: StickyFilterBarProps) {
  const [isSticky, setIsSticky] = useState(false)

  useEffect(() => {
    const handleScroll = () => {
      // Become sticky after scrolling past header (~64px)
      setIsSticky(window.scrollY > 64)
    }

    window.addEventListener("scroll", handleScroll, { passive: true })
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  return (
    <div
      className={cn(
        "sticky top-0 z-40 transition-all duration-200",
        isSticky && "bg-background/95 backdrop-blur-sm border-b shadow-sm",
        className
      )}
    >
      <div className="container mx-auto px-4 py-3">
        {children}
      </div>
    </div>
  )
}
```

### Filter Bar Content

```typescript
// components/explore/ExploreFilters.tsx
export function ExploreFilters({
  selectedType,
  selectedLeague,
  searchQuery,
  onTypeChange,
  onLeagueChange,
  onSearchChange,
}: ExploreFiltersProps) {
  return (
    <StickyFilterBar>
      <div className="flex flex-col sm:flex-row gap-3 sm:items-center sm:justify-between">
        {/* Type filter pills */}
        <div className="flex gap-2">
          <FilterPill
            active={selectedType === "all"}
            onClick={() => onTypeChange("all")}
          >
            All
          </FilterPill>
          <FilterPill
            active={selectedType === "result_streaks"}
            onClick={() => onTypeChange("result_streaks")}
          >
            Results
          </FilterPill>
          <FilterPill
            active={selectedType === "goal_streaks"}
            onClick={() => onTypeChange("goal_streaks")}
          >
            Goals
          </FilterPill>
        </div>

        {/* League dropdown + Search */}
        <div className="flex gap-2">
          <LeagueDropdown
            value={selectedLeague}
            onChange={onLeagueChange}
          />
          <SearchInput
            value={searchQuery}
            onChange={onSearchChange}
            placeholder="Search teams..."
          />
        </div>
      </div>
    </StickyFilterBar>
  )
}
```

### CSS Alternative (Simpler)

```css
/* In globals.css or component */
.filter-bar {
  position: sticky;
  top: 0;
  z-index: 40;
  background: hsl(var(--background) / 0.95);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid transparent;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.filter-bar.scrolled {
  border-color: hsl(var(--border));
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}
```

---

## 5. League Quick-Chips

### Problem
Opening a dropdown to filter by league adds friction. Users often want to quickly toggle between a few leagues.

### Solution
Horizontal scrollable chips showing top leagues for one-tap filtering.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  ← [All] [🏴󠁧󠁢󠁥󠁮󠁧󠁿 EPL] [🇪🇸 La Liga] [🇩🇪 Bundesliga] [🇮🇹 Serie A] [More ▼] →  │
└─────────────────────────────────────────────────┘
     ↑ Horizontally scrollable on mobile
```

### Component Implementation

```typescript
// components/ui/LeagueChips.tsx
"use client"

import { cn } from "@/lib/utils"
import { ChevronDown } from "lucide-react"

interface League {
  id: string
  name: string
  shortName: string
  logo: string
  countryFlag: string
}

const TOP_LEAGUES: League[] = [
  { id: "all", name: "All Leagues", shortName: "All", logo: "", countryFlag: "" },
  { id: "premier-league", name: "Premier League", shortName: "EPL", logo: "...", countryFlag: "🏴󠁧󠁢󠁥󠁮󠁧󠁿" },
  { id: "la-liga", name: "La Liga", shortName: "La Liga", logo: "...", countryFlag: "🇪🇸" },
  { id: "bundesliga", name: "Bundesliga", shortName: "Bundesliga", logo: "...", countryFlag: "🇩🇪" },
  { id: "serie-a", name: "Serie A", shortName: "Serie A", logo: "...", countryFlag: "🇮🇹" },
  { id: "ligue-1", name: "Ligue 1", shortName: "Ligue 1", logo: "...", countryFlag: "🇫🇷" },
]

interface LeagueChipsProps {
  selected: string
  onChange: (leagueId: string) => void
  className?: string
}

export function LeagueChips({ selected, onChange, className }: LeagueChipsProps) {
  return (
    <div
      className={cn(
        "flex gap-2 overflow-x-auto scrollbar-hide pb-2 -mb-2",
        className
      )}
    >
      {TOP_LEAGUES.map((league) => (
        <button
          key={league.id}
          onClick={() => onChange(league.id)}
          className={cn(
            "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors",
            "border",
            selected === league.id
              ? "bg-primary text-primary-foreground border-primary"
              : "bg-background hover:bg-muted border-border"
          )}
        >
          {league.countryFlag && (
            <span className="text-base">{league.countryFlag}</span>
          )}
          <span>{league.shortName}</span>
        </button>
      ))}

      {/* More dropdown for additional leagues */}
      <button
        className={cn(
          "flex items-center gap-1 px-3 py-1.5 rounded-full text-sm font-medium",
          "border border-border bg-background hover:bg-muted"
        )}
      >
        More
        <ChevronDown className="w-3.5 h-3.5" />
      </button>
    </div>
  )
}
```

### Hide Scrollbar CSS

```css
/* globals.css */
.scrollbar-hide {
  -ms-overflow-style: none;
  scrollbar-width: none;
}

.scrollbar-hide::-webkit-scrollbar {
  display: none;
}
```

### Usage on Home Page

```typescript
// In SportsStreaks or AppHeader
<LeagueChips
  selected={selectedLeague}
  onChange={(id) => setSelectedLeague(id)}
  className="mt-4"
/>
```

---

## 6. Opponent Logo Display

### Problem
Text-only opponent name ("vs Newcastle") requires reading. Logo recognition is faster.

### Solution
Show opponent team logo alongside name.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  Next Match:                                    │
│  [Newcastle Logo] Newcastle United              │
│  Tomorrow, 3:00 PM                              │
└─────────────────────────────────────────────────┘
```

### Data Requirement

The backend needs to provide `next_opponent_logo_url` in the streak data:

```typescript
// Updated StreakData type
export interface StreakData {
  // ... existing fields
  next_opponent: string
  next_opponent_logo_url?: string  // NEW
  next_match_date: string
}
```

### Component Implementation

```typescript
// components/ui/OpponentBadge.tsx
import Image from "next/image"
import { cn } from "@/lib/utils"

interface OpponentBadgeProps {
  name: string
  logoUrl?: string
  matchDate?: string
  className?: string
}

export function OpponentBadge({ name, logoUrl, matchDate, className }: OpponentBadgeProps) {
  return (
    <div className={cn("flex items-center gap-2", className)}>
      {logoUrl ? (
        <Image
          src={logoUrl}
          alt={name}
          width={20}
          height={20}
          className="w-5 h-5 object-contain"
        />
      ) : (
        <div className="w-5 h-5 rounded-full bg-muted" />
      )}
      <span className="text-sm">
        <span className="text-muted-foreground">vs </span>
        <span className="font-medium">{name}</span>
      </span>
    </div>
  )
}
```

### Fallback Strategy

If `next_opponent_logo_url` is not available:
1. Show placeholder circle
2. Or fetch from a logo mapping on the frontend
3. Or omit logo and show text only

---

## 7. Empty State with Suggestions

### Problem
When filters return no results, users see a blank page with no guidance.

### Solution
Show helpful empty state with suggestions to broaden the search.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│                                                 │
│            🔍                                   │
│                                                 │
│     No streaks found                            │
│                                                 │
│     No goal streaks in La Liga match            │
│     your current filters.                       │
│                                                 │
│     Try:                                        │
│     • [View Result Streaks] ← clickable        │
│     • [View All Leagues]                        │
│     • [Clear all filters]                       │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Component Implementation

```typescript
// components/ui/EmptyState.tsx
import { Search } from "lucide-react"
import { Button } from "@/components/ui/button"

interface Suggestion {
  label: string
  action: () => void
}

interface EmptyStateProps {
  title?: string
  description: string
  suggestions?: Suggestion[]
  onClearFilters?: () => void
}

export function EmptyState({
  title = "No streaks found",
  description,
  suggestions = [],
  onClearFilters,
}: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <div className="w-12 h-12 rounded-full bg-muted flex items-center justify-center mb-4">
        <Search className="w-6 h-6 text-muted-foreground" />
      </div>

      <h3 className="text-lg font-semibold mb-2">{title}</h3>
      <p className="text-muted-foreground max-w-sm mb-6">{description}</p>

      {(suggestions.length > 0 || onClearFilters) && (
        <div className="flex flex-col gap-2">
          <p className="text-sm text-muted-foreground mb-2">Try:</p>
          {suggestions.map((suggestion, index) => (
            <Button
              key={index}
              variant="outline"
              size="sm"
              onClick={suggestion.action}
            >
              {suggestion.label}
            </Button>
          ))}
          {onClearFilters && (
            <Button
              variant="ghost"
              size="sm"
              onClick={onClearFilters}
            >
              Clear all filters
            </Button>
          )}
        </div>
      )}
    </div>
  )
}
```

### Usage in Explore Page

```typescript
// In ExploreContent
if (streaks.length === 0 && !loading) {
  const suggestions: Suggestion[] = []

  if (selectedType === "goal_streaks") {
    suggestions.push({
      label: "View Result Streaks",
      action: () => setSelectedType("result_streaks"),
    })
  }

  if (selectedLeague !== "all") {
    suggestions.push({
      label: "View All Leagues",
      action: () => setSelectedLeague("all"),
    })
  }

  return (
    <EmptyState
      description={`No ${selectedType === "goal_streaks" ? "goal" : "result"} streaks found${
        selectedLeague !== "all" ? ` in ${selectedLeagueName}` : ""
      }.`}
      suggestions={suggestions}
      onClearFilters={clearAllFilters}
    />
  )
}
```

---

## 8. Active Filter Summary

### Problem
Users forget what filters are active, especially after scrolling through results.

### Solution
Show active filters as dismissible pills below the filter bar.

### Design Spec

```
┌─────────────────────────────────────────────────┐
│  [All] [Results] [Goals]  |  [League ▼] [🔍]   │
├─────────────────────────────────────────────────┤
│  Active: [Goal Streaks ×] [La Liga ×] [Clear]  │  ← Filter summary
├─────────────────────────────────────────────────┤
│  [Streak Card]                                  │
```

### Component Implementation

```typescript
// components/explore/ActiveFilterPills.tsx
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

export function ActiveFilterPills({ filters, onClearAll, className }: ActiveFilterPillsProps) {
  if (filters.length === 0) return null

  return (
    <div className={cn("flex items-center gap-2 flex-wrap", className)}>
      <span className="text-sm text-muted-foreground">Active:</span>

      {filters.map((filter) => (
        <button
          key={filter.key}
          onClick={filter.onRemove}
          className={cn(
            "inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium",
            "bg-primary/10 text-primary hover:bg-primary/20 transition-colors"
          )}
        >
          {filter.label}
          <X className="w-3 h-3" />
        </button>
      ))}

      {filters.length > 1 && (
        <button
          onClick={onClearAll}
          className="text-xs text-muted-foreground hover:text-foreground underline"
        >
          Clear all
        </button>
      )}
    </div>
  )
}
```

### Usage

```typescript
// Build active filters array
const activeFilters: ActiveFilter[] = []

if (selectedType !== "all") {
  activeFilters.push({
    key: "type",
    label: selectedType === "goal_streaks" ? "Goal Streaks" : "Result Streaks",
    onRemove: () => setSelectedType("all"),
  })
}

if (selectedLeague !== "all") {
  activeFilters.push({
    key: "league",
    label: leagues.find((l) => l.id === selectedLeague)?.name || selectedLeague,
    onRemove: () => setSelectedLeague("all"),
  })
}

if (searchQuery) {
  activeFilters.push({
    key: "search",
    label: `"${searchQuery}"`,
    onRemove: () => setSearchQuery(""),
  })
}

// Render
<ActiveFilterPills
  filters={activeFilters}
  onClearAll={clearAllFilters}
  className="mt-3"
/>
```

---

## Implementation Priority

| # | Feature | Effort | Impact | Priority |
|---|---------|--------|--------|----------|
| 1 | Form Dots Visualization | Low | High | P0 |
| 2 | Scope Badge Prominence | Low | High | P0 |
| 3 | Match Countdown | Low | High | P0 |
| 4 | Sticky Filter Bar | Medium | High | P1 |
| 5 | League Quick-Chips | Medium | Medium | P1 |
| 6 | Empty State with Suggestions | Low | Medium | P1 |
| 7 | Active Filter Summary | Low | Medium | P2 |
| 8 | Opponent Logo Display | Low | Low | P2 |

---

## File Structure for New Components

```
components/
├── ui/
│   ├── ScopeBadge.tsx          # NEW
│   ├── FormDots.tsx            # NEW
│   ├── MatchCountdown.tsx      # NEW
│   ├── OpponentBadge.tsx       # NEW
│   └── EmptyState.tsx          # NEW (or update existing)
│
├── explore/
│   ├── StickyFilterBar.tsx     # NEW
│   ├── ExploreFilters.tsx      # UPDATE
│   ├── ActiveFilterPills.tsx   # NEW
│   └── LeagueChips.tsx         # NEW (or in ui/)
│
└── lib/
    └── formatMatchTime.ts      # NEW
```

---

## Testing Checklist

- [ ] ScopeBadge renders correctly for all 3 scope values
- [ ] ScopeBadge colors work in light and dark mode
- [ ] FormDots shows correct colors for W/D/L
- [ ] FormDots has proper accessibility labels
- [ ] MatchCountdown shows "Today" for today's matches
- [ ] MatchCountdown shows "Tomorrow" correctly
- [ ] MatchCountdown shows "Starting soon" within 2 hours
- [ ] StickyFilterBar becomes sticky after scrolling
- [ ] StickyFilterBar has blur effect when sticky
- [ ] LeagueChips horizontal scroll works on mobile
- [ ] LeagueChips selection state updates correctly
- [ ] EmptyState shows relevant suggestions
- [ ] ActiveFilterPills dismisses filters on click
- [ ] All components work in dark mode

---

*Document ready for implementation. Last updated: January 2025*
