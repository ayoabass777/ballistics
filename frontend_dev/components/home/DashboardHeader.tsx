"use client"

import { useState } from "react"
import {
  Search,
  X,
  Crosshair,
  Menu,
  Zap,
  TrendingUp,
  Globe,
  Trophy,
  ChevronRight,
  ChevronDown,
  User,
} from "lucide-react"

interface DashboardHeaderProps {
  searchQuery: string
  setSearchQuery: (query: string) => void
  totalStreaks: number
  pageType?: "home" | "explore"
}

const leaguesByCountry = [
  {
    country: "England",
    flag: "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    leagues: [
      { id: "premier-league", name: "Premier League", logo: "https://media.api-sports.io/football/leagues/39.png" },
    ],
  },
  {
    country: "Spain",
    flag: "🇪🇸",
    leagues: [{ id: "la-liga", name: "La Liga", logo: "https://media.api-sports.io/football/leagues/140.png" }],
  },
  {
    country: "Germany",
    flag: "🇩🇪",
    leagues: [{ id: "bundesliga", name: "Bundesliga", logo: "https://media.api-sports.io/football/leagues/78.png" }],
  },
  {
    country: "Italy",
    flag: "🇮🇹",
    leagues: [{ id: "serie-a", name: "Serie A", logo: "https://media.api-sports.io/football/leagues/135.png" }],
  },
  {
    country: "France",
    flag: "🇫🇷",
    leagues: [{ id: "ligue-1", name: "Ligue 1", logo: "https://media.api-sports.io/football/leagues/61.png" }],
  },
]

type HeaderLeagueOption = {
  id: string
  name: string
  logo: string | null
}

const allLeaguesOption: HeaderLeagueOption = { id: "all", name: "All Leagues", logo: null }

export function DashboardHeader({
  searchQuery,
  setSearchQuery,
  totalStreaks,
  pageType = "home",
}: DashboardHeaderProps) {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [leagueDropdownOpen, setLeagueDropdownOpen] = useState(false)
  const [selectedLeague, setSelectedLeague] = useState<HeaderLeagueOption>(allLeaguesOption)
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null)

  const quickStats = [
    { icon: TrendingUp, value: `${totalStreaks}+`, label: "Active Streaks" },
    { icon: Globe, value: "10", label: "Leagues" },
    { icon: Trophy, value: "250+", label: "Teams Tracked" },
  ]

  return (
    <div className="mb-12 animate-fade-in-up">
      <nav className="fixed top-0 left-0 right-0 z-50 px-4 py-3">
        <div className="max-w-7xl mx-auto">
          <div className="relative backdrop-blur-2xl bg-gradient-to-r from-slate-900/90 via-slate-800/85 to-slate-900/90 dark:from-black/80 dark:via-slate-900/75 dark:to-black/80 border border-white/10 rounded-2xl px-4 md:px-6 py-3 shadow-2xl shadow-black/20 overflow-visible">
            <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-blue-400/50 to-transparent" />
            <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent" />

            <div className="relative flex items-center justify-between gap-4">
              <div className="flex items-center gap-2.5 flex-shrink-0">
                <div className="relative p-2 rounded-xl bg-gradient-to-br from-blue-500/30 to-cyan-500/20 border border-blue-400/30 shadow-lg shadow-blue-500/20">
                  <Crosshair className="h-5 w-5 text-blue-400" />
                </div>
                <span className="font-bold text-white tracking-tight text-lg hidden sm:block">Ballistics</span>
              </div>

              {/* League Selector */}
              <div className="relative hidden md:block">
                <button
                  onClick={() => setLeagueDropdownOpen(!leagueDropdownOpen)}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 hover:border-white/20 transition-all duration-200"
                >
                  {selectedLeague.logo && (
                    <img src={selectedLeague.logo || "/placeholder.svg"} alt="" className="h-5 w-5 object-contain" />
                  )}
                  {!selectedLeague.logo && <Globe className="h-5 w-5 text-slate-400" />}
                  <span className="text-sm font-medium text-white">{selectedLeague.name}</span>
                  <ChevronDown
                    className={`h-4 w-4 text-slate-400 transition-transform duration-200 ${leagueDropdownOpen ? "rotate-180" : ""}`}
                  />
                </button>

                {leagueDropdownOpen && (
                  <div className="absolute top-full left-0 mt-2 w-72 rounded-xl bg-slate-900/95 backdrop-blur-xl border border-white/10 shadow-2xl shadow-black/40 overflow-hidden z-[100]">
                    <button
                      onClick={() => {
                        setSelectedLeague(allLeaguesOption)
                        setLeagueDropdownOpen(false)
                      }}
                      className={`w-full flex items-center gap-3 px-4 py-3 text-sm text-left transition-all duration-150 border-b border-white/5 ${
                        selectedLeague.id === allLeaguesOption.id
                          ? "bg-gradient-to-r from-blue-500/20 to-cyan-500/20 text-blue-300"
                          : "text-slate-300 hover:bg-white/5 hover:text-white"
                      }`}
                    >
                      <div className="flex items-center justify-center w-6 h-6 rounded-lg bg-gradient-to-br from-blue-500/20 to-cyan-500/20 border border-blue-400/30">
                        <Globe className="h-4 w-4 text-blue-400" />
                      </div>
                      <span className="font-medium">{allLeaguesOption.name}</span>
                    </button>

                    <div className="py-2 max-h-96 overflow-y-auto">
                      {leaguesByCountry.map((countryGroup) => (
                        <div key={countryGroup.country} className="mb-1 last:mb-0">
                          <button
                            onClick={() =>
                              setExpandedCountry(expandedCountry === countryGroup.country ? null : countryGroup.country)
                            }
                            className="w-full px-4 py-2.5 flex items-center justify-between gap-2 hover:bg-white/5 transition-all duration-150"
                          >
                            <div className="flex items-center gap-2">
                              <span className="text-lg">{countryGroup.flag}</span>
                              <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
                                {countryGroup.country}
                              </span>
                            </div>
                            <ChevronRight
                              className={`h-4 w-4 text-slate-500 transition-transform duration-200 ${expandedCountry === countryGroup.country ? "rotate-90" : ""}`}
                            />
                          </button>

                          <div
                            className={`overflow-hidden transition-all duration-200 ${expandedCountry === countryGroup.country ? "max-h-96 opacity-100" : "max-h-0 opacity-0"}`}
                          >
                            {countryGroup.leagues.map((league) => (
                              <button
                                key={league.id}
                                onClick={() => {
                                  setSelectedLeague(league)
                                  setLeagueDropdownOpen(false)
                                  setExpandedCountry(null)
                                }}
                                className={`w-full flex items-center gap-3 px-4 pl-10 py-2.5 text-sm text-left transition-all duration-150 ${
                                  selectedLeague.id === league.id
                                    ? "bg-gradient-to-r from-blue-500/15 to-cyan-500/15 text-blue-300 border-l-2 border-blue-400"
                                    : "text-slate-300 hover:bg-white/5 hover:text-white hover:pl-11"
                                }`}
                              >
                                <img
                                  src={league.logo || "/placeholder.svg"}
                                  alt=""
                                  className="h-5 w-5 object-contain"
                                />
                                <span className="font-medium">{league.name}</span>
                              </button>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <div className="flex-1 max-w-md">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
                  <input
                    type="text"
                    placeholder="Search teams, leagues..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-10 pr-4 py-2 text-sm rounded-xl bg-white/5 border border-white/10 text-white placeholder:text-slate-500 focus:outline-none focus:border-blue-400/50 focus:bg-white/10 transition-all duration-200"
                  />
                  {searchQuery && (
                    <button
                      onClick={() => setSearchQuery("")}
                      className="absolute right-3 top-1/2 -translate-y-1/2 p-0.5 rounded-full hover:bg-white/10 transition-colors"
                    >
                      <X className="h-3.5 w-3.5 text-slate-400" />
                    </button>
                  )}
                </div>
              </div>

              <button className="hidden sm:flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-300 hover:text-white rounded-xl hover:bg-white/5 transition-all duration-200">
                <User className="h-4 w-4" />
                <span>Sign In</span>
              </button>

              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="md:hidden p-2 rounded-lg hover:bg-white/10 transition-colors text-white"
              >
                <Menu className="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>
      </nav>

      <div className="pt-28 text-center mb-10">
        <div className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-blue-500/10 border border-blue-400/20 mb-6">
          <Zap className="h-4 w-4 text-blue-400" />
          <span className="text-sm font-medium text-blue-400">Real-time Streak Analysis</span>
        </div>

        {pageType === "home" ? (
          <>
            <h1 className="text-5xl md:text-6xl font-bold mb-4 tracking-tight text-balance text-foreground">
              Predict the Game
              <br />
              <span className="bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
                Understand the Form
              </span>
            </h1>

            <p className="text-muted-foreground text-lg md:text-xl font-light max-w-2xl mx-auto mb-8">
              Ballistics tracks team streaks across top leagues to reveal patterns behind match outcomes.
            </p>
          </>
        ) : (
          <>
            <h1 className="text-5xl md:text-6xl font-bold mb-4 tracking-tight text-balance text-foreground">
              Explore Streaks
              <br />
              <span className="bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
                Discover the Data
              </span>
            </h1>

            <p className="text-muted-foreground text-lg md:text-xl font-light max-w-2xl mx-auto mb-8">
              Filter and search through all active streaks to find the insights that matter to you.
            </p>
          </>
        )}

        <div className="flex flex-wrap justify-center gap-4 md:gap-8 mb-10">
          {quickStats.map((stat, index) => (
            <div
              key={index}
              className="flex items-center gap-3 px-5 py-3 rounded-xl bg-background/50 backdrop-blur-sm border border-border/50 hover:border-blue-400/30 transition-all duration-300 hover:scale-105"
            >
              <div className="p-2 rounded-lg bg-blue-500/10">
                <stat.icon className="h-5 w-5 text-blue-400" />
              </div>
              <div className="text-left">
                <p className="text-xl font-bold text-foreground">{stat.value}</p>
                <p className="text-xs text-muted-foreground">{stat.label}</p>
              </div>
            </div>
          ))}
        </div>

        <div className="flex flex-wrap justify-center gap-4 mb-10">
          <button className="group px-6 py-3 text-base font-semibold rounded-xl bg-gradient-to-r from-blue-500 to-cyan-500 text-white hover:from-blue-400 hover:to-cyan-400 transition-all duration-200 shadow-lg shadow-blue-500/30 hover:shadow-blue-500/40 hover:scale-105 active:scale-95 flex items-center gap-2">
            Explore Streaks
            <ChevronRight className="h-4 w-4 group-hover:translate-x-1 transition-transform" />
          </button>
          <button className="px-6 py-3 text-base font-semibold rounded-xl bg-background/50 backdrop-blur-sm border border-border/50 text-foreground hover:bg-background/80 hover:border-blue-400/30 transition-all duration-200">
            How It Works
          </button>
        </div>
      </div>
    </div>
  )
}
