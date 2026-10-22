# Ballistics - System Architecture

> A real-time football analytics platform focused on tracking winning streaks across major European leagues.

**Version**: MVP
**Branch**: `mvp_dev`
**Last Updated**: January 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Technology Stack](#2-technology-stack)
3. [Directory Structure](#3-directory-structure)
4. [Component Architecture](#4-component-architecture)
5. [Data Flow](#5-data-flow)
6. [State Management](#6-state-management)
7. [Navigation System](#7-navigation-system)
8. [API Contract](#8-api-contract)
9. [Type System](#9-type-system)
10. [Configuration](#10-configuration)
11. [Future TODOs](#11-future-todos)

---

## 1. Overview

### 1.1 What is Ballistics?

Ballistics is a Next.js web application that provides real-time streak analysis for football (soccer) matches across major European leagues. It helps users identify patterns in team performance by tracking consecutive events like wins and goal-scoring runs.

### 1.2 Target Users

| User Type | Primary Use Case |
|-----------|------------------|
| **Sports Bettors** | Analyze streak patterns to inform betting decisions |
| **Casual Fans** | Track favorite teams and discover interesting trends |

### 1.3 Core Domain Concepts

```
┌─────────────────────────────────────────────────────────────┐
│                         STREAK                              │
├─────────────────────────────────────────────────────────────┤
│  A consecutive sequence of matches where a team achieves    │
│  a specific outcome (e.g., "5 consecutive wins")            │
└─────────────────────────────────────────────────────────────┘
         │
         ├── Streak Types (MVP)
         │   ├── result_streaks  → Win, Loss, Draw, Win or Draw
         │   └── goal_streaks    → Score 1/2/3 Goals, Concede Goals
         │
         ├── Scope
         │   ├── overall → All matches
         │   ├── home    → Home matches only
         │   └── away    → Away matches only
         │
         └── Context
             ├── Team       → The team with the streak
             ├── League     → Competition (Premier League, La Liga, etc.)
             ├── Season     → Identified by season_id (composite key in DB)
             ├── Fixture    → Upcoming match where streak continues/breaks
             └── Opponent   → Next opponent team
```

### 1.4 Leagues Covered

| League | Country | Coverage |
|--------|---------|----------|
| Premier League | England | Full |
| Championship | England | Full |
| Bundesliga | Germany | Full |
| Serie A | Italy | Full |
| La Liga | Spain | Full |
| Ligue 1 | France | Full |

### 1.5 Page Strategy

| Page | Purpose | Streak Limit | Endpoint |
|------|---------|--------------|----------|
| **Home** | Showcase top streaks to entice exploration | Max 20 streaks | `GET /api/home/streaks` |
| **Explore** | Browse all streaks with filters | Infinite scroll | `GET /api/explore/streaks` |
| **Team** | Team-specific streak analysis | All team streaks | `GET /api/team/:slug` |
| **Fixture** | Match preview with streak context | Relevant streaks | `GET /api/fixture/:id` |

---

## 2. Technology Stack

### 2.1 Core Framework

| Technology | Version | Purpose |
|------------|---------|---------|
| **Next.js** | 15.5.9 | React meta-framework with App Router |
| **React** | 19 | UI library |
| **TypeScript** | 5 | Type safety |
| **Node.js** | 18+ | Runtime |

### 2.2 Styling & UI

| Technology | Purpose |
|------------|---------|
| **Tailwind CSS** | Utility-first CSS (v4 with PostCSS) |
| **Radix UI** | Accessible UI primitives |
| **shadcn/ui** | Pre-built component library (New York style) |
| **Lucide React** | Icon library |
| **CVA** | Class variance authority for component variants |

### 2.3 Data Management

| Technology | Purpose |
|------------|---------|
| **SWR** | Data fetching with caching and revalidation |
| **React Hook Form** | Form state management |
| **Zod** | Schema validation |

### 2.4 Additional Libraries

| Library | Purpose |
|---------|---------|
| next-themes | Dark/light theme management |
| sonner | Toast notifications |
| recharts | Data visualization |
| date-fns | Date utilities |
| embla-carousel | Carousel component |

### 2.5 Deployment

| Service | Purpose |
|---------|---------|
| **Vercel** | Hosting and deployment |

---

## 3. Directory Structure

```
frontend_dev/
│
├── app/                              # Next.js App Router
│   ├── page.tsx                      # Home page (→ SportsStreaks)
│   ├── layout.tsx                    # Root layout with Providers
│   ├── globals.css                   # Global Tailwind styles
│   │
│   ├── explore/                      # /explore route
│   │   ├── page.tsx                  # Explore all streaks (infinite scroll)
│   │   └── loading.tsx               # Loading skeleton
│   │
│   ├── league/[slug]/                # /league/:slug route
│   │   └── page.tsx                  # League standings + streaks
│   │
│   ├── team/[slug]/                  # /team/:slug route
│   │   └── page.tsx                  # Team detail + streaks
│   │
│   ├── fixture/[fixtureId]/          # /fixture/:id route
│   │   ├── page.tsx                  # Server component
│   │   └── FixtureAnalysisPageClient.tsx  # Client component
│   │
│   └── api/                          # API Routes
│       ├── streaks/route.ts          # GET /api/streaks
│       └── team/[slug]/route.ts      # GET /api/team/:slug
│
├── components/                       # React Components
│   ├── home/                         # Home page components
│   ├── explore/                      # Explore page components
│   ├── league/                       # League page components
│   ├── team/                         # Team page components
│   ├── fixture/                      # Fixture page components
│   ├── ui/                           # shadcn/ui components
│   └── utils/                        # Utility components
│
├── lib/                              # Utilities & Hooks
│   ├── hooks/                        # Custom React hooks
│   ├── data/                         # Data layer abstraction
│   │   ├── team-data.ts              # Team data service
│   │   ├── fixture-data.ts           # Fixture data service
│   │   └── mock/                     # Mock data for development
│   │       ├── team-mock-data.ts     # Team mock data
│   │       └── fixture-mock-data.ts  # Fixture mock data
│   ├── mock-streaks.ts               # Mock streak data (legacy)
│   ├── mock-fixture-data.ts          # Mock fixture data (legacy)
│   ├── swr-config.ts                 # SWR configuration
│   └── utils.ts                      # Utility functions (cn)
│
├── types/                            # TypeScript definitions
│   ├── index.ts                      # Core types
│   ├── team-page.ts                  # Team page view model
│   ├── fixture-page.ts               # Fixture page view model
│   └── explore-page.ts               # Explore page view model
│
├── sports-streaks.tsx                # Main home component
│
├── docs/                             # Documentation
│   └── ARCHITECTURE.md               # This file
│
└── Configuration Files
    ├── package.json
    ├── tsconfig.json
    ├── next.config.mjs
    ├── postcss.config.mjs
    ├── components.json               # shadcn/ui config
    └── .env.local                    # Environment variables
```

---

## 4. Component Architecture

### 4.1 Component Hierarchy Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           app/layout.tsx                            │
│                         (Root Layout)                               │
├─────────────────────────────────────────────────────────────────────┤
│  <Providers>                                                        │
│    ├── ErrorBoundary                                                │
│    ├── ThemeProvider (dark/light)                                   │
│    ├── SWRConfig (global fetch config)                              │
│    └── ToastProvider                                                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│   Home (/)    │         │  Explore        │         │  League         │
│  Max 20       │         │  (/explore)     │         │  (/league/:id)  │
│  streaks      │         │  Infinite       │         │                 │
├───────────────┤         │  Scroll         │         ├─────────────────┤
│ SportsStreaks │         ├─────────────────┤         │ LeaguePageShell │
│  ├─ AppHeader │         │ ExploreContent  │         │  ├─ LeagueHeader│
│  ├─ HeroCTA   │         │  ├─ FloatingFilt│         │  ├─ LeagueStreak│
│  ├─ QuickStats│         │  ├─ FilterPills │         │  └─ LeagueTable │
│  └─ StreakGrid│         │  ├─ SearchBar   │         │                 │
│               │         │  ├─ StreakGrid  │         │                 │
│  CTA: Explore │         │  └─ Sentinel ◄──┼── Intersection Observer   │
└───────────────┘         └─────────────────┘         └─────────────────┘
        │                           │                           │
        └───────────────────────────┴───────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Team (/team/:slug)                          │
├─────────────────────────────────────────────────────────────────────┤
│  TeamPageShell                                                      │
│    ├── TeamHeader                                                   │
│    │     ├── LeagueSeasonLabel (includes season_id)                 │
│    │     ├── LeagueTeamSwitcher                                     │
│    │     └── FormDots (Last 5 form visualization)                   │
│    ├── UpcomingMatchBadge (optional)                                │
│    │     └── MatchCountdown (relative time display)                 │
│    ├── CurrentStreaks                                               │
│    │     ├── Context filters (All/Home/Away)                        │
│    │     ├── Category filters (All/Results/Goals)                   │
│    │     ├── StreakMiniCard (×N)                                    │
│    │     └── EmptyState (when filters return 0 results)             │
│    └── Footer CTAs                                                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Fixture (/fixture/:id)                          │
├─────────────────────────────────────────────────────────────────────┤
│  FixtureAnalysisPageClient                                          │
│    ├── FixtureHeader (back navigation)                              │
│    ├── FixtureMatchupHero (team vs team)                            │
│    ├── TeamAvgGoals (per scope: overall/home/away)                  │
│    ├── Team Form Section                                            │
│    │     └── StreakItem (×N)                                        │
│    ├── H2H Section                                                  │
│    │     ├── H2HOverview                                            │
│    │     ├── H2HStreakItem (×N)                                     │
│    │     └── RecentMeetings                                         │
│    └── FixturePageSkeleton (loading)                                │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Server vs Client Components

Next.js 15 App Router uses a hybrid rendering model:

| Component Type | Characteristics | Examples |
|----------------|-----------------|----------|
| **Server Components** | Async, fetch data, read params | `app/*/page.tsx` |
| **Client Components** | Interactive, use hooks, state | `*Shell.tsx`, `*Client.tsx` |

**Boundary Pattern:**

```typescript
// Server Component (app/team/[slug]/page.tsx)
export default async function TeamPage({ params, searchParams }) {
  const { slug } = await params
  const { returnTo } = await searchParams
  const data = await fetchTeamData(slug)

  // Pass resolved data to Client Component
  return <TeamPageShell viewModel={data} returnTo={returnTo} />
}

// Client Component (components/team/TeamPageShell.tsx)
"use client"
export function TeamPageShell({ viewModel, returnTo }) {
  const [filter, setFilter] = useState("overall")
  // Interactive logic...
}
```

### 4.3 Infinite Scroll Pattern (Explore Page)

The explore page uses Intersection Observer for infinite scrolling:

```typescript
// Sentinel element at bottom of streak list
const sentinelRef = useRef<HTMLDivElement>(null)
const [page, setPage] = useState(1)

useEffect(() => {
  const observer = new IntersectionObserver(
    (entries) => {
      // When sentinel becomes visible, load more
      if (entries[0].isIntersecting && !loading && hasMore) {
        setPage((prev) => prev + 1)
      }
    },
    { threshold: 0.1 }
  )

  if (sentinelRef.current) {
    observer.observe(sentinelRef.current)
  }

  return () => observer.disconnect()
}, [loading, hasMore])

// In JSX:
<div className="grid ...">
  {streaks.map((streak) => <StreakCard key={streak.streak_id} {...streak} />)}
</div>
<div ref={sentinelRef} className="h-10" /> {/* Sentinel */}
{loading && <LoadingSkeleton />}
```

### 4.4 Shared Components

| Component | Purpose | Used By |
|-----------|---------|---------|
| `StreakGrid` | Display streak cards in grid | Home, Explore |
| `StreakCard` | Individual streak display | StreakGrid |
| `StreakMiniCard` | Compact streak card | Team page |
| `FormDots` | Visual form strip (W/D/L) | Team header |
| `MatchCountdown` | Relative time display | UpcomingMatchBadge |
| `EmptyState` | Empty state with clear filters | CurrentStreaks |
| `LoadingSkeleton` | Loading placeholder | All pages |
| `ThemeToggle` | Dark/light mode switch | Headers |
| `ErrorBoundary` | Error catching | Root layout |

---

## 5. Data Flow

### 5.1 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BACKEND (In Development)                     │
│                                                                     │
│   Football Data API → Processing → Database → REST API              │
│                                                                     │
│   Handles:                                                          │
│   • min_length filtering                                            │
│   • Pagination (limit/offset for infinite scroll)                   │
│   • Data aggregation                                                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                  │
│                                                                     │
│   lib/data/team-data.ts ─────────┬─── getTeamBySlug()               │
│   lib/data/fixture-data.ts ──────┤─── getFixtureById()              │
│                                  └─── getAllFixtureIds()            │
│                                                                     │
│   Currently: Returns mock data from lib/data/mock/                  │
│   Future: Will call backend API when ready                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          API ROUTES                                 │
│                                                                     │
│   /api/streaks ──────────────────┬──────────────────────────────   │
│   /api/team/[slug] ──────────────┤                                  │
│   /api/fixtures/[id] (planned) ──┘                                  │
│                                                                     │
│   Currently: Returns mock data from lib/mock-*.ts (legacy)          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CUSTOM HOOKS (SWR)                           │
│                                                                     │
│   useStreakData() ─────┐                                            │
│   useFixtureData() ────┼─── SWR Cache ─── Automatic Revalidation    │
│   useStreakFilter() ───┘        │                                   │
│   useStreakSearch() ────────────┘                                   │
│                                                                     │
│   Features:                                                         │
│   • Caching with stale-while-revalidate                             │
│   • Auto-refresh (5 min streaks, 2 min fixtures)                    │
│   • Error retry with exponential backoff                            │
│   • Mock data fallback via NEXT_PUBLIC_USE_MOCK_DATA                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PAGE COMPONENTS                                │
│                                                                     │
│   Home (max 20) ───────┐                                            │
│   Explore (infinite) ──┼─── Apply filters, search, transformations  │
│   TeamPageShell ───────┤                                            │
│   FixtureClient ───────┘                                            │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      UI COMPONENTS                                  │
│                                                                     │
│   StreakGrid → StreakCard                                           │
│   CurrentStreaks → StreakMiniCard                                   │
│   LeagueTable → Team rows                                           │
│   H2HOverview → Stats display                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 SWR Configuration

```typescript
// lib/swr-config.ts
export const swrConfig: SWRConfiguration = {
  revalidateOnFocus: true,          // Refresh when tab regains focus
  revalidateOnReconnect: true,      // Refresh after network recovery
  revalidateIfStale: false,         // Don't auto-revalidate if data exists
  dedupingInterval: 2000,           // Dedupe requests within 2s
  focusThrottleInterval: 300000,    // Throttle focus revalidation (5 min)
  errorRetryCount: 3,               // Retry failed requests 3 times
  errorRetryInterval: 5000,         // Wait 5s between retries
  keepPreviousData: true,           // Show stale data while fetching

  fetcher: async (url) => {
    const response = await fetch(url)
    if (!response.ok) throw new Error("Fetch failed")
    return response.json()
  }
}
```

### 5.3 Hook-Specific Configuration

| Hook | Refresh Interval | Notes |
|------|------------------|-------|
| `useStreakData` | 5 minutes | Home: max 20 streaks |
| `useFixtureData` | 2 minutes | Match-specific data |

### 5.4 Data Layer Abstraction

The data layer provides a clean abstraction between pages and data sources. This allows easy swapping between mock data (development) and real API calls (production).

**Team Data Service (`lib/data/team-data.ts`):**

```typescript
export interface TeamDataService {
  getTeamBySlug(slug: string): Promise<TeamPageViewModel | null>
}

// Currently uses mock data
export async function getTeamBySlug(slug: string): Promise<TeamPageViewModel | null> {
  const { MOCK_TEAMS } = await import("./mock/team-mock-data")
  return MOCK_TEAMS[slug] || null
}
```

**Fixture Data Service (`lib/data/fixture-data.ts`):**

```typescript
export interface FixtureDataService {
  getFixtureById(fixtureId: string): Promise<FixturePageViewModel | null>
}

export async function getFixtureById(fixtureId: string): Promise<FixturePageViewModel | null> {
  const { MOCK_FIXTURES } = await import("./mock/fixture-mock-data")
  return MOCK_FIXTURES[fixtureId] || null
}

export async function getAllFixtureIds(): Promise<string[]> {
  const { MOCK_FIXTURES } = await import("./mock/fixture-mock-data")
  return Object.keys(MOCK_FIXTURES)
}
```

**Usage in Pages:**

```typescript
// app/team/[slug]/page.tsx
import { getTeamBySlug } from "@/lib/data/team-data"

export default async function TeamPage({ params }) {
  const { slug } = await params
  const viewModel = await getTeamBySlug(slug)
  // ...
}
```

### 5.5 Mock Data Toggle (Legacy)

```typescript
// Controlled via environment variable
if (process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true") {
  return { streaks: mockStreaks, loading: false, error: null }
}
// Otherwise use SWR with real API
```

---

## 6. State Management

### 6.1 State Management Strategy

```
┌─────────────────────────────────────────────────────────────────────┐
│                     STATE MANAGEMENT LAYERS                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────┐                                                │
│  │   URL State     │  Filters, search queries, returnTo             │
│  │  (searchParams) │  → Shareable, bookmarkable                     │
│  └────────┬────────┘                                                │
│           │                                                         │
│  ┌────────▼────────┐                                                │
│  │   Server State  │  Streak data, fixture data, team data          │
│  │     (SWR)       │  → Cached, auto-revalidated                    │
│  └────────┬────────┘                                                │
│           │                                                         │
│  ┌────────▼────────┐                                                │
│  │   Local State   │  UI state (filters, modals, dropdowns)         │
│  │   (useState)    │  → Component-scoped                            │
│  └────────┬────────┘                                                │
│           │                                                         │
│  ┌────────▼────────┐                                                │
│  │  Session State  │  Scroll position                               │
│  │(sessionStorage) │  → Tab-scoped, survives navigation             │
│  └────────┬────────┘                                                │
│           │                                                         │
│  ┌────────▼────────┐                                                │
│  │  Context State  │  Theme (dark/light)                            │
│  │ (React Context) │  → App-wide                                    │
│  └─────────────────┘                                                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 6.2 URL State (Explore Page)

```typescript
// URL: /explore?type=result_streaks&league=premier-league&q=arsenal

// Reading state
const searchParams = useSearchParams()
const type = searchParams.get("type") || "all"
const league = searchParams.get("league") || "all"
const query = searchParams.get("q") || ""

// Updating state
const updateFilters = (newType: string) => {
  const params = new URLSearchParams(searchParams)
  params.set("type", newType)
  router.replace(`/explore?${params.toString()}`)
}
```

### 6.3 Scroll Position Preservation

```typescript
const SCROLL_KEY = "explore_scroll_position"

// Save before navigation
const saveScrollPosition = () => {
  sessionStorage.setItem(SCROLL_KEY, window.scrollY.toString())
}

// Restore on mount
useEffect(() => {
  const saved = sessionStorage.getItem(SCROLL_KEY)
  if (saved) {
    requestAnimationFrame(() => window.scrollTo(0, parseInt(saved)))
    sessionStorage.removeItem(SCROLL_KEY)
  }
}, [])
```

---

## 7. Navigation System

### 7.1 Route Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                          ROUTE STRUCTURE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   /                         Home page (max 20 streaks)              │
│   │                         CTA → "Explore More Streaks"            │
│   │                                                                 │
│   ├── /explore              All streaks with infinite scroll        │
│   │   ?type=...             Streak type filter                      │
│   │   &league=...           League filter                           │
│   │   &q=...                Search query                            │
│   │                                                                 │
│   ├── /league/:slug         League detail page                      │
│   │                                                                 │
│   ├── /team/:slug           Team detail page                        │
│   │   ?returnTo=...         Encoded return URL                      │
│   │                                                                 │
│   └── /fixture/:id          Fixture analysis page                   │
│       ?returnTo=...         Encoded return URL                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.2 returnTo Navigation Pattern

The `returnTo` parameter enables contextual back navigation with preserved state.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        returnTo FLOW                                 │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   1. SOURCE PAGE                                                     │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  // Home page                                                │    │
│   │  onClick={() => {                                            │    │
│   │    const returnUrl = encodeURIComponent("/")                 │    │
│   │    router.push(`/fixture/${id}?returnTo=${returnUrl}`)       │    │
│   │  }}                                                          │    │
│   │                                                              │    │
│   │  // Explore page (with filters)                              │    │
│   │  onClick={() => {                                            │    │
│   │    saveScrollPosition()                                      │    │
│   │    const returnUrl = encodeURIComponent(                     │    │
│   │      `/explore?type=${type}&league=${league}&q=${query}`     │    │
│   │    )                                                         │    │
│   │    router.push(`/team/${slug}?returnTo=${returnUrl}`)        │    │
│   │  }}                                                          │    │
│   └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│   2. SERVER COMPONENT (reads searchParams)                           │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  const { returnTo } = await searchParams                     │    │
│   │  const decoded = returnTo ? decodeURIComponent(returnTo) : null │ │
│   │  return <TeamPageShell returnTo={decoded} />                 │    │
│   └─────────────────────────────────────────────────────────────┘    │
│                              │                                       │
│                              ▼                                       │
│   3. CLIENT COMPONENT (uses returnTo)                                │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  const handleBack = () => {                                  │    │
│   │    if (returnTo) {                                           │    │
│   │      router.push(returnTo)  // Returns with state preserved  │    │
│   │    } else {                                                  │    │
│   │      router.back()          // Browser history fallback      │    │
│   │    }                                                         │    │
│   │  }                                                           │    │
│   └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 7.3 Navigation Matrix

| From | To | returnTo Value | Back Label | Notes |
|------|-----|----------------|------------|-------|
| Home | Team | `/` | "Back to Home" | |
| Home | Fixture | `/` | "Back to Home" | |
| Explore | Team | `/explore?type=...&league=...` | "Back to Explore" | Scroll saved |
| Explore | Fixture | `/explore?type=...&league=...` | "Back to Explore" | Hide explore link |
| League | Team | `/league/:slug` | "Back to League" | |
| League | Fixture | `/league/:slug` | "Back to League" | |
| Team | Fixture | `/team/:slug` | "Back" | |
| Direct URL | Any | `null` | "Back" | Uses history.back() |

---

## 8. API Contract

### 8.1 Current Endpoints (Mock)

#### `GET /api/home/streaks`

Returns curated streaks for home page (max 20, sorted by significance).

**Response:**
```typescript
{
  streaks: StreakData[]
  meta: {
    total: number
    generatedAtIso: string
  }
}
```

#### `GET /api/explore/streaks`

Returns all streaks for explore page with pagination support.

**Response:**
```typescript
{
  streaks: StreakData[]
  meta: {
    total: number
    hasMore: boolean
    generatedAtIso: string
  }
}
```

#### `GET /api/team/:slug`

Returns team detail with streaks and avg goals per scope.

### 8.2 Backend Integration Contract

When the backend is ready, these contracts apply:

#### `GET /api/home/streaks`

Returns top streaks for home page (curated, max 20).

**Query Parameters:**
| Parameter | Type | Description | Handled By |
|-----------|------|-------------|------------|
| `limit` | `number` | Max streaks to return (default: 20) | Backend |

**Note:** Backend handles curation, sorting by significance, and min_length filtering.

**Response:**
```typescript
{
  streaks: StreakData[]
  meta: {
    total: number
    generatedAtIso: string
  }
}
```

#### `GET /api/explore/streaks`

Returns all streaks for explore page with filtering and pagination.

**Query Parameters:**
| Parameter | Type | Description | Handled By |
|-----------|------|-------------|------------|
| `type` | `string` | `result_streaks` or `goal_streaks` | Frontend filter |
| `league` | `string` | Filter by league season_id | Frontend filter |
| `scope` | `string` | `overall`, `home`, `away` | Frontend filter |
| `q` | `string` | Search query (team name, league) | Frontend filter |
| `offset` | `number` | Pagination offset (for infinite scroll) | Backend |
| `limit` | `number` | Page size (default: 20) | Backend |

**Note:** `min_length` is handled by backend.

**Response:**
```typescript
{
  streaks: StreakData[]
  filters: {
    leagues: LeagueOption[]    // Available leagues with counts
    types: StreakTypeOption[]  // Available types with counts
  }
  meta: {
    total: number
    hasMore: boolean
    generatedAtIso: string
  }
}
```

#### `GET /api/fixtures/:id`

**Response:**
```typescript
{
  fixture: {
    id: string
    homeTeam: TeamIdentity
    awayTeam: TeamIdentity
    date: string              // ISO 8601
    league: LeagueInfo
    venue: string
  }
  homeStreaks: StreakData[]
  awayStreaks: StreakData[]
  h2h: {
    totalMatches: number
    homeWins: number
    awayWins: number
    draws: number
    recentMeetings: Meeting[]
  }
  avgGoals: {
    home: {
      overall: { for: number, against: number }
      home: { for: number, against: number }
      away: { for: number, against: number }
    }
    away: {
      overall: { for: number, against: number }
      home: { for: number, against: number }
      away: { for: number, against: number }
    }
  }
}
```

---

## 9. Type System

### 9.1 Core Types (`types/index.ts`)

```typescript
// Streak result type
export type MatchResult = "W" | "D" | "L"

// Streak scope
export type StreakScope = "overall" | "home" | "away"

// Streak type (MVP: only result and goal)
export type StreakType = "result_streaks" | "goal_streaks"

// Streak category for UI (maps to database category column)
export type StreakCategory = "result" | "goal"

// Streak polarity - indicates whether the streak is positive or negative for the team
// Examples:
//   positive: wins, clean sheets, scoring goals, unbeaten runs
//   negative: losses, winless runs, conceding goals, goalless runs
export type StreakPolarity = "positive" | "negative"

// League filter for UI
export interface LeagueFilter {
  id: string
  name: string
  logo: string | null
}
```

### 9.2 Streak Data Structure

```typescript
// Streak polarity - indicates whether the streak is positive or negative for the team
export type StreakPolarity = "positive" | "negative"

// Core streak data from backend
export interface StreakData {
  streak_id: string
  fixture_id: string

  // Team info
  team_name: string
  team_logo_url: string

  // League info
  league_name: string
  league_logo_url: string
  country_name: string
  country_logo_url: string

  // Streak details
  event_name: string                    // "Win", "Score 2 Goals"
  streak_type: StreakType               // "result_streaks" | "goal_streaks"
  metric_key: MetricKey                 // Database metric identifier
  current_streak_length: number
  scope: StreakScope                    // "overall" | "home" | "away"
  polarity: StreakPolarity              // "positive" | "negative"

  // Form & next match
  recent_form: MatchResult[]            // ["W", "W", "D", "W", "L"]
  next_opponent: string
  next_match_date: string               // ISO 8601
}
```

### 9.3 Metric Keys

The `metric_key` identifies the specific streak type:

```typescript
// All possible metric keys from backend
export type MetricKey =
  // Goal streaks - Scoring
  | "score_1goal"        // Goals for >= 1
  | "score_2goals"       // Goals for >= 2
  | "goalless"           // Goals for = 0
  | "no_scoring"         // Goals for = 0

  // Goal streaks - Conceding
  | "concede_1"          // Goals against >= 1
  | "concede_2"          // Goals against >= 2
  | "clean_sheet"        // Goals against = 0

  // Goal streaks - Total goals (Over/Under)
  | "Over_1_5"           // Total goals > 1.5
  | "Over_2_5"           // Total goals > 2.5
  | "Over_3_5"           // Total goals > 3.5
  | "Over_4_5"           // Total goals > 4.5
  | "Under_1_5"          // Total goals < 1.5
  | "Under_2_5"          // Total goals < 2.5
  | "Under_3_5"          // Total goals < 3.5
  | "Under_4_5"          // Total goals < 4.5

  // Goal streaks - Both teams
  | "BTTS"               // Both teams to score

  // Result streaks - Full time
  | "win"                // Full time win
  | "draw"               // Full time draw
  | "loss"               // Full time loss
  | "unbeaten"           // Win or draw
  | "winless"            // Draw or loss

  // Result streaks - Half time
  | "halftime_win"       // Half time win
```

### 9.4 Streak Card View Model

```typescript
// View model for streak card component
export interface StreakCardVM {
  streakId: string
  teamId: string
  fixtureId: string

  category: StreakCategory              // "result" | "goal"
  scope: StreakScope                    // "overall" | "home" | "away"

  metricKey: MetricKey                  // e.g., "win", "score_2goals", "Over_2_5"
  length: number                        // Current streak length
  polarity: StreakPolarity              // "positive" | "negative"

  headline: string                      // "5 Consecutive Wins"
}
```

### 9.5 Team & League Types

```typescript
// Team identity
export interface TeamIdentity {
  id: string
  name: string
  shortName?: string
  logo: string
  primaryColor?: string                 // TODO: Generate via OpenAI
}

// League/Season context
export interface LeagueSeasonContext {
  seasonId: number                      // Backend composite key (e.g., 58, 98)
  leagueId: string
  leagueName: string
  leagueLogo: string
  season: string                        // "2024/25"
  countryName?: string
  countryFlag?: string
}

// Average goals per scope
export interface TeamAvgGoals {
  overall: { for: number, against: number }
  home: { for: number, against: number }
  away: { for: number, against: number }
}
```

### 9.6 Form Window

```typescript
// Form window options (FUTURE: form window selector not yet implemented in UI)
// Currently the UI displays a fixed "Last 5" form strip in TeamHeader
export interface FormWindow {
  key: "LAST_5" | "LAST_10" | "SEASON"
  label: string
  matchCount: number                      // e.g. 5, 10, or current played for SEASON
}
```

**Current Implementation:**
- TeamHeader displays "Last 5:" with form dots (hardcoded)
- Uses `form.lastN.slice(0, 5)` for display

**Future Enhancement:**
- Form window selector UI (dropdown/tabs)
- Dynamic form stats based on selected window

### 9.7 Team Page View Model (`types/team-page.ts`)

The team page uses a comprehensive view model that encapsulates all data needed for rendering.

```typescript
export type TeamPageViewModel = {
  // Identity + routing anchors
  team: TeamIdentity
  context: LeagueSeasonContext

  // Form window (fixed to Last 5 for MVP)
  // W/D/L summary + lastN outcomes (no charts)
  form: TeamFormSummary

  // Headline streaks (the "drivers" shown prominently)
  currentStreaks: {
    primary: StreakCardVM[]               // 3–6 max
    secondary?: StreakCardVM[]            // Optional extra row (shown when uiHints.showSecondaryStreakRow is true)
  }

  // Streak explorer preview (limited lists by category)
  streakExplorer: {
    activeCategory: StreakCategory
    categories: Array<{
      category: StreakCategory
      label: string
      items: StreakCardVM[]               // 6–10 max per category
      viewAllHref: string                 // Team-filtered + category-filtered
    }>
    viewAllTeamStreaksHref: string        // Team-filtered (all categories)
  }

  // Neutral next match context (optional; NO comparison)
  upcomingMatch?: UpcomingMatchVM

  // Top-level CTAs for the page shell
  ctas: {
    backToLeagueHref: string
    exploreStreaksHref: string            // Global streaks page
    shareHref?: string
  }

  // Optional flags to simplify rendering rules
  uiHints?: {
    showUpcomingMatch: boolean
    showSecondaryStreakRow: boolean
  }

  // Metadata
  generatedAtIso: string
}
```

**Supporting Types:**

```typescript
// Team identity for team page
export type TeamIdentity = {
  teamId: string
  name: string
  shortName?: string
  crestUrl?: string
  rank?: number
}

// League/Season context with team switcher data
export type LeagueSeasonContext = {
  leagueSeasonId: string                  // Combined identifier (e.g., "premier-league-2025-26")
  leagueName: string
  seasonLabel: string                     // "2025/26"
  countryName?: string
  leagueTeams: Array<{                    // For team switcher dropdown
    teamId: string
    name: string
    teamHref: string
  }>
}

// Form summary (MVP: simplified to Last 5 only)
export type TeamFormSummary = {
  // Outcomes only (compact strip) - Last 5
  lastN: Array<"W" | "D" | "L">
}

// Streak category for team page
export type StreakCategory = "result" | "goals"

// Streak card view model for team page
export type StreakCardVM = {
  streakId: string
  teamId: string

  category: StreakCategory                // "result" | "goals"
  polarity: "positive" | "negative" | "neutral"
  scope?: "overall" | "home" | "away"

  metricKey: string                       // Deterministic canonical key
  threshold: number                       // e.g. 1, 2, 3 (for "2+")
  length: number                          // Current streak length

  headline: string                        // e.g. "Unbeaten in 6"
}

// Upcoming match info
export type UpcomingMatchVM = {
  matchId: string
  kickoffIso?: string

  isHome: boolean
  opponent: {
    teamId: string
    name: string
    crestUrl?: string
    teamHref: string
    rank?: number
  }

  venue?: string
}
```

### 9.8 Explore Page Data Types (`types/explore-page.ts`)

The explore page uses a comprehensive view model for browsing all streaks.

```typescript
export type ExplorePageViewModel = {
  // All active streaks
  streaks: ExploreStreakVM[]

  // Available filters (populated from streaks data)
  filters: {
    leagues: LeagueOption[]
    types: StreakTypeOption[]
  }

  // Metadata
  totalCount: number
  generatedAtIso: string
}

// Supporting types
export type StreakType = "goal_streaks" | "result_streaks"
export type StreakScope = "overall" | "home" | "away"
export type StreakResult = "W" | "D" | "L"
export type StreakPolarity = "positive" | "negative" | "neutral"

// Streak item for explore page
export type ExploreStreakVM = {
  // Identifiers
  streakId: string
  fixtureId?: string                    // For navigation to fixture page

  // Streak info
  eventName: string                     // "Win", "Clean Sheet", "Score 2 Goals"
  streakType: StreakType
  streakLength: number
  scope?: StreakScope
  polarity?: StreakPolarity

  // Team info
  team: {
    teamId: string
    name: string
    logoUrl?: string
    elo?: number
  }

  // League info
  league: {
    leagueId: string
    name: string
    logoUrl?: string
    country?: string
    countryLogoUrl?: string
  }

  // Next match context
  nextMatch?: {
    date: string                        // ISO date
    opponent: string
    opponentElo?: number
  }

  // Optional extras
  secondaryStreak?: string
  recentForm?: StreakResult[]
}

// League filter option
export type LeagueOption = {
  id: string                            // URL-friendly slug
  name: string                          // Display name
  logoUrl?: string
  country?: string
  countryLogoUrl?: string
  count: number                         // Number of streaks in this league
}

// Streak type filter option
export type StreakTypeOption = {
  id: StreakType
  label: string                         // Display label (e.g., "Results", "Goals")
  count: number                         // Number of streaks of this type
}

// Current filter state for URL params
export type ExploreFilters = {
  type?: StreakType | "all"
  league?: string                       // League slug or "all"
  search?: string
}
```

### 9.9 Fixture Page Data Types (`types/fixture-page.ts`)

The fixture analysis page displays pre-match analysis for an upcoming fixture.

```typescript
export type FixturePageViewModel = {
  // Fixture identity
  fixture: FixtureIdentity

  // League/season context
  league: LeagueContext

  // Kickoff information
  kickoff: KickoffInfo

  // Venue details
  venue: VenueInfo

  // Team identities with stats
  homeTeam: FixtureTeam
  awayTeam: FixtureTeam

  // Team streaks heading into the match
  homeStreaks: FixtureStreak[]
  awayStreaks: FixtureStreak[]

  // Head-to-head statistics
  h2hStats: H2HStats

  // Recent form for both teams
  homeTeamForm: FormMatch[]
  awayTeamForm: FormMatch[]

  // Top-level CTAs for the page shell
  ctas: {
    backHref?: string
    shareHref?: string
    homeTeamHref: string
    awayTeamHref: string
  }

  // Metadata
  generatedAtIso: string
}

// Core fixture identity
export type FixtureIdentity = {
  fixtureId: string
  slug?: string                           // URL-friendly identifier
}

// League and season context for the fixture
export type LeagueContext = {
  leagueSeasonId: string                  // Combined identifier (e.g., "premier-league-2025-26")
  name: string
  logoUrl: string
  country: string
  countryLogoUrl?: string
  matchday?: number
}

// Kickoff time information (frontend handles timezone conversion)
export type KickoffInfo = {
  iso: string                             // Full ISO 8601 datetime (e.g., "2026-01-15T20:00:00Z")
}

// Venue information
export type VenueInfo = {
  name: string
  city: string
  capacity?: number
  imageUrl?: string
}

// Team identity and stats for fixture context
export type FixtureTeam = {
  teamId: string
  name: string
  shortName?: string
  slug: string                            // For navigation to team page
  logoUrl: string
  primaryColor?: string

  // Average goals stats (legacy - overall)
  avgGoalsScored: number
  avgGoalsConceded: number

  // Average goals by context (for context filter)
  avgGoals?: {
    overall: AvgGoalsStats
    home: AvgGoalsStats
    away: AvgGoalsStats
  }

  // Recent form results (W/D/L) for last 5 matches
  recentForm?: Array<"W" | "D" | "L">

  // Recent form by context
  formByContext?: {
    overall: Array<"W" | "D" | "L">
    home: Array<"W" | "D" | "L">
    away: Array<"W" | "D" | "L">
  }
}

// Streak item in fixture context
export type FixtureStreak = {
  event: string                           // Human-readable event name (e.g., "Won at home")
  count: number                           // Current streak length
  total?: number                          // Total matches in scope (e.g., "4 of last 10")
  scope: StreakScope                      // "overall" | "home" | "away"
}

// Head-to-head statistics
export type H2HStats = {
  totalMatches: number
  homeWins: number                        // Current home team wins in H2H
  draws: number
  awayWins: number                        // Current away team wins in H2H

  // Recent meetings list
  recentMatches: H2HMatch[]

  // H2H-specific streaks
  streaks: H2HStreak[]
}

// Individual H2H match result
export type H2HMatch = {
  date: string                            // ISO date
  home: number                            // Home team score
  away: number                            // Away team score
  result: "H" | "D" | "A"                 // Home win, Draw, Away win
  isHomeTeamHome: boolean                 // Was current fixture's home team at home?
}

// H2H-specific streak
export type H2HStreak = {
  event: string                           // e.g., "Both teams scored"
  count: number
  total?: number
  teams: "home" | "away" | "both"         // Which team(s) the streak applies to
}

// Individual form match result
export type FormMatch = {
  date: string                            // ISO date
  opponent: string                        // Opponent name
  score: string                           // e.g., "2-1"
  result: "W" | "L" | "D"
  isHome: boolean
}
```

**Fixture Page Component Structure:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Fixture (/fixture/:id)                          │
├─────────────────────────────────────────────────────────────────────┤
│  FixtureAnalysisPageClient                                          │
│    │                                                                │
│    ├── FixtureHeader                                                │
│    │     └── Back button (uses returnTo or history.back)            │
│    │                                                                │
│    ├── FixtureMatchupHero                                           │
│    │     ├── Home team logo + name                                  │
│    │     ├── "vs" separator                                         │
│    │     ├── Away team logo + name                                  │
│    │     └── Kickoff date/time + venue                              │
│    │                                                                │
│    ├── TeamAvgGoals                                                 │
│    │     └── Average goals for/against per scope                    │
│    │                                                                │
│    ├── Team Form Section                                            │
│    │     └── StreakItem (×N for both teams)                         │
│    │                                                                │
│    ├── H2H Section                                                  │
│    │     ├── H2HOverview (wins/draws/losses pie)                    │
│    │     └── H2HStreakItem (×N)                                     │
│    │                                                                │
│    ├── RecentMeetings                                               │
│    │     └── List of recent H2H results                             │
│    │                                                                │
│    └── Floating Share Button                                        │
│                                                                     │
│  Data Flow:                                                         │
│    useFixtureData(fixtureId) → SWR cache → FixtureData              │
│    - Refresh interval: 2 minutes (time-sensitive)                   │
│    - Falls back to mockFixture during development                   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. Configuration

### 10.1 Environment Variables

```bash
# .env.local

# Toggle mock data (set to "true" for development without backend)
NEXT_PUBLIC_USE_MOCK_DATA=true

# Future: Backend API URL
# NEXT_PUBLIC_API_URL=https://api.ballistics.app

# Future: OpenAI API for team color generation
# OPENAI_API_KEY=sk-...
```

### 10.2 TypeScript Configuration

```json
// tsconfig.json
{
  "compilerOptions": {
    "target": "ES6",
    "strict": true,
    "moduleResolution": "bundler",
    "paths": {
      "@/*": ["./*"]
    }
  }
}
```

### 10.3 Next.js Configuration

```javascript
// next.config.mjs
const nextConfig = {
  eslint: {
    ignoreDuringBuilds: true
  },
  typescript: {
    ignoreBuildErrors: true
  },
  images: {
    unoptimized: true
  }
}
```

---

## 11. Future TODOs

### 11.1 Team Primary Color Generation

Generate team primary colors using OpenAI text API based on team name.

**Implementation:**

```typescript
// lib/generateTeamColor.ts
import OpenAI from 'openai'

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
})

export async function generateTeamPrimaryColor(teamName: string): Promise<string> {
  const response = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content: "You are a sports branding expert. Given a football team name, return their primary brand color as a hex code. Only respond with the hex code, nothing else."
      },
      {
        role: "user",
        content: `What is the primary brand color for ${teamName}?`
      }
    ],
    max_tokens: 10
  })

  return response.choices[0].message.content?.trim() || "#000000"
}

// Usage:
// const color = await generateTeamPrimaryColor("Manchester United")
// Returns: "#DA291C"
```

**Caching Strategy:**
- Store generated colors in database
- Only generate once per team
- Fallback to default color if API fails

### 11.2 Planned Features

| Feature | Priority | Status |
|---------|----------|--------|
| Team primary color via OpenAI | Medium | TODO |
| User authentication (sign in) | High | Planned |
| Historical H2H data | Medium | Planned |
| Notifications/alerts | Low | Future |

### 11.3 Backend Integration Checklist

- [x] Create data layer abstraction (`lib/data/`) for easy API swapping
- [x] Implement `getTeamBySlug()` and `getFixtureById()` data services
- [x] Move mock data to dedicated `lib/data/mock/` directory
- [x] Add comprehensive fixture page types (`types/fixture-page.ts`)
- [ ] Replace mock data implementations with real API calls
- [ ] Implement infinite scroll with backend pagination
- [ ] Add error handling for API failures
- [ ] Cache team colors after generation
- [ ] Implement season_id in all league/team contexts

### 11.4 Performance Optimizations

- [ ] Implement Intersection Observer for infinite scroll
- [ ] Add service worker for offline support
- [ ] Optimize images with Next.js Image component
- [ ] Add prefetching for predictable navigation paths

---

## Appendix A: Component Reference

### A.1 Home Page Components

| Component | File | Purpose |
|-----------|------|---------|
| `SportsStreaks` | `sports-streaks.tsx` | Main container (max 20 streaks) |
| `AppHeader` | `components/home/AppHeader.tsx` | Navigation, search |
| `HeroCTA` | `components/home/HeroCTA.tsx` | Hero + Explore CTA |
| `QuickStats` | `components/home/QuickStats.tsx` | Summary statistics |

### A.2 Explore Page Components

| Component | File | Purpose |
|-----------|------|---------|
| `ExploreContent` | `app/explore/page.tsx` | Infinite scroll container |
| `FloatingLeagueFilter` | `components/explore/FloatingLeagueFilter.tsx` | Filter UI |

### A.3 Team Page Components

| Component | File | Purpose |
|-----------|------|---------|
| `TeamPageShell` | `components/team/TeamPageShell.tsx` | Page container with header, content, footer CTAs |
| `TeamHeader` | `components/team/TeamHeader.tsx` | Team identity, FormDots (Last 5), league context |
| `LeagueSeasonLabel` | `components/team/LeagueSeasonLabel.tsx` | League name + season badge |
| `LeagueTeamSwitcher` | `components/team/LeagueTeamSwitcher.tsx` | Dropdown to switch between teams in league |
| `CurrentStreaks` | `components/team/CurrentStreaks.tsx` | Filterable streak display with context/category filters, EmptyState |
| `StreakMiniCard` | `components/team/StreakMiniCard.tsx` | Compact streak card with polarity indicator |
| `UpcomingMatchBadge` | `components/team/UpcomingMatchBadge.tsx` | Next match info with MatchCountdown + opponent |

### A.4 Shared UI Components

| Component | File | Purpose |
|-----------|------|---------|
| `FormDots` | `components/ui/FormDots.tsx` | Visual form strip showing W/D/L as colored dots |
| `MatchCountdown` | `components/ui/MatchCountdown.tsx` | Relative time display for upcoming matches |
| `EmptyState` | `components/ui/EmptyState.tsx` | Empty state with optional clear filters action |

### A.5 Fixture Page Components

| Component | File | Purpose |
|-----------|------|---------|
| `FixtureAnalysisPageClient` | `app/fixture/[fixtureId]/FixtureAnalysisPageClient.tsx` | Main client component with all sections |
| `FixtureHeader` | `components/fixture/FixtureHeader.tsx` | Back navigation with returnTo support |
| `FixtureMatchupHero` | `components/fixture/FixtureMatchupHero.tsx` | Team vs team hero with logos, kickoff, venue |
| `TeamAvgGoals` | `components/fixture/TeamAvgGoals.tsx` | Average goals per scope (overall/home/away) |
| `StreakItem` | `components/fixture/StreakItem.tsx` | Individual team streak in form section |
| `H2HOverview` | `components/fixture/H2HOverview.tsx` | Head-to-head summary stats |
| `H2HStreakItem` | `components/fixture/H2HStreakItem.tsx` | H2H-specific streak item |
| `RecentMeetings` | `components/fixture/RecentMeetings.tsx` | Recent H2H match results |
| `FixturePageSkeleton` | `components/fixture/FixturePageSkeleton.tsx` | Loading skeleton |

---

## Appendix B: Custom Hooks Reference

| Hook | File | Purpose |
|------|------|---------|
| `useStreakData` | `lib/hooks/useStreakData.ts` | Fetch streaks |
| `useStreakFilter` | `lib/hooks/useStreakFilter.ts` | Filter by league |
| `useStreakSearch` | `lib/hooks/useStreakSearch.ts` | Debounced search |
| `useFixtureData` | `lib/hooks/useFixtureData.ts` | Fetch fixture |
| `useURLParams` | `lib/hooks/useURLParams.ts` | URL state |

---

*Document maintained by the development team. Last updated: January 2026*
