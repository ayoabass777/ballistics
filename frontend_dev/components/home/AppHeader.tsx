"use client"

import Link from "next/link"
import { SignedIn, SignedOut, SignInButton, SignUpButton, UserButton } from "@clerk/nextjs"
import { useRouter } from "next/navigation"
import { useEffect, useRef, useState } from "react"
import { TrendingUp, Crosshair, Search, X, Globe, ChevronDown, ChevronRight, Menu } from "lucide-react"
import { ThemeToggle } from "@/components/theme-toggle"
import type { NavCountryGroup, NavLeagueOption } from "@/types/navigation"
import type { TeamSearchSuggestion } from "@/types/search"
import { useTeamSearchAutocomplete } from "@/lib/hooks"
import { TeamSearchSuggestions } from "@/components/home/TeamSearchSuggestions"

type LeagueSelection = Pick<NavLeagueOption, "id" | "name" | "logo"> & {
  href?: string
}

interface AppHeaderProps {
  searchQuery?: string
  onSearchChange?: (query: string) => void
  selectedLeague?: { id: string; name: string; logo: string | null }
  onLeagueChange?: (league: { id: string; name: string; logo: string | null }) => void
  leagueGroups?: NavCountryGroup[]
  navLoading?: boolean
  navError?: string | null
}


const allLeaguesOption = { id: "all", name: "All Leagues", logo: null }

export function AppHeader({
  searchQuery = "",
  onSearchChange,
  selectedLeague,
  onLeagueChange,
  leagueGroups = [],
  navLoading = false,
  navError = null,
}: AppHeaderProps) {
  const [leagueDropdownOpen, setLeagueDropdownOpen] = useState(false)
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [searchSuggestionsOpen, setSearchSuggestionsOpen] = useState(false)
  const router = useRouter()
  const searchContainerRef = useRef<HTMLDivElement | null>(null)
  const currentLeague = selectedLeague || allLeaguesOption
  const {
    suggestions: teamSuggestions,
    loading: teamSearchLoading,
    error: teamSearchError,
  } = useTeamSearchAutocomplete(searchQuery)

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!searchContainerRef.current) return
      if (!searchContainerRef.current.contains(event.target as Node)) {
        setSearchSuggestionsOpen(false)
      }
    }

    document.addEventListener("mousedown", handlePointerDown)
    return () => document.removeEventListener("mousedown", handlePointerDown)
  }, [])

  const handleLeagueSelect = (league: LeagueSelection) => {
    if (league.id === "all") {
      onLeagueChange?.(league)
    } else {
      router.push(league.href || `/league/${league.id}`)
    }
    setLeagueDropdownOpen(false)
    setMobileMenuOpen(false)
    setExpandedCountry(null)
  }

  const handleTeamSuggestionSelect = (suggestion: TeamSearchSuggestion) => {
    setSearchSuggestionsOpen(false)
    router.push(suggestion.href)
  }

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 px-2 sm:px-4 py-2 sm:py-3" aria-label="Main navigation">
      <div className="max-w-7xl mx-auto">
        <div className="relative backdrop-blur-xl bg-background/85 dark:bg-background/80 border border-border/50 dark:border-border/40 rounded-xl sm:rounded-2xl px-3 sm:px-4 md:px-6 py-2 sm:py-3 shadow-lg shadow-black/5 dark:shadow-black/20 overflow-visible">
          <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-primary/30 to-transparent" aria-hidden="true" />
          <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-border/50 to-transparent" aria-hidden="true" />

          <div className="relative flex items-center justify-between gap-2">
            {/* Logo */}
            <Link href="/" className="flex items-center gap-1.5 sm:gap-2.5 flex-shrink-0 group" aria-label="Ballistics home">
              <div className="relative p-1.5 sm:p-2 rounded-lg sm:rounded-xl bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 transition-all duration-300 group-hover:scale-105 group-hover:bg-primary/15 dark:group-hover:bg-primary/25">
                <Crosshair className="h-4 w-4 sm:h-5 sm:w-5 text-primary" />
              </div>
              <span className="font-bold text-foreground tracking-tight text-base sm:text-lg hidden sm:block">
                Ballistics
              </span>
            </Link>

            <div className="flex items-center gap-2 flex-1 max-w-xs sm:max-w-sm md:max-w-md lg:max-w-xl">
              <div ref={searchContainerRef} className="relative flex-1 min-w-0">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" aria-hidden="true" />
                <input
                  type="search"
                  placeholder="Search teams..."
                  value={searchQuery}
                  onFocus={() => setSearchSuggestionsOpen(true)}
                  onChange={(e) => {
                    onSearchChange?.(e.target.value)
                    setSearchSuggestionsOpen(true)
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Escape") {
                      setSearchSuggestionsOpen(false)
                      return
                    }
                    if (e.key === "Enter" && teamSuggestions.length > 0) {
                      e.preventDefault()
                      handleTeamSuggestionSelect(teamSuggestions[0])
                    }
                  }}
                  aria-label="Search streaks"
                  className="w-full pl-8 pr-8 py-1.5 sm:py-2 text-xs sm:text-sm rounded-lg sm:rounded-xl bg-secondary/60 dark:bg-secondary/40 border border-border/50 text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary/40 focus:ring-2 focus:ring-primary/20 focus:bg-secondary/80 dark:focus:bg-secondary/60 transition-all duration-200"
                />
                {searchQuery && (
                  <button
                    onClick={() => {
                      onSearchChange?.("")
                      setSearchSuggestionsOpen(false)
                    }}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 rounded-full hover:bg-secondary transition-colors"
                    aria-label="Clear search"
                  >
                    <X className="h-3 w-3 text-muted-foreground" />
                  </button>
                )}
                <TeamSearchSuggestions
                  query={searchQuery}
                  suggestions={teamSuggestions}
                  loading={teamSearchLoading}
                  error={teamSearchError}
                  visible={searchSuggestionsOpen && (searchQuery.trim().length > 0 || teamSearchLoading)}
                  onSelect={handleTeamSuggestionSelect}
                />
              </div>

              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="md:hidden p-3 rounded-lg bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-border transition-all duration-200 min-h-[44px] min-w-[44px] flex items-center justify-center"
                aria-label="Open mobile menu"
                aria-expanded={mobileMenuOpen}
              >
                <Menu className="h-5 w-5 text-foreground" />
              </button>

              {/* Desktop league selector */}
              <div className="relative hidden md:block">
                <button
                  onClick={() => setLeagueDropdownOpen(!leagueDropdownOpen)}
                  className="flex items-center gap-2 px-3 py-2 rounded-xl bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-border transition-all duration-200 min-h-[44px]"
                  aria-label="Select league"
                  aria-expanded={leagueDropdownOpen}
                  aria-haspopup="true"
                >
                  {currentLeague.logo && (
                    <span className="flex h-5 w-5 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
                      <img src={currentLeague.logo || "/placeholder.svg"} alt="" className="h-4 w-4 object-contain" />
                    </span>
                  )}
                  {!currentLeague.logo && <Globe className="h-4 w-4 text-muted-foreground" />}
                  <span className="text-xs font-medium text-foreground">{currentLeague.name}</span>
                  <ChevronDown
                    className={`h-3.5 w-3.5 text-muted-foreground transition-transform duration-200 ${leagueDropdownOpen ? "rotate-180" : ""}`}
                  />
                </button>

                {leagueDropdownOpen && (
                  <div
                    className="absolute top-full left-0 mt-2 w-72 rounded-xl bg-popover/98 backdrop-blur-xl border border-border/60 shadow-xl shadow-black/10 dark:shadow-black/30 overflow-hidden z-[100]"
                    role="menu"
                    aria-label="League selection menu"
                  >
                    <button
                      onClick={() => handleLeagueSelect(allLeaguesOption)}
                      className={`w-full flex items-center gap-3 px-4 py-3 text-sm text-left transition-all duration-150 border-b border-border/40 ${
                        currentLeague.id === allLeaguesOption.id
                          ? "bg-primary/10 dark:bg-primary/20 text-primary"
                          : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40"
                      }`}
                    >
                      <div className="flex items-center justify-center w-6 h-6 rounded-lg bg-primary/10 dark:bg-primary/20 border border-primary/20">
                        <Globe className="h-4 w-4 text-primary" />
                      </div>
                      <span className="font-medium">{allLeaguesOption.name}</span>
                    </button>

                    <div className="py-2 max-h-96 overflow-y-auto">
                      {leagueGroups.map((countryGroup) => (
                        <div key={countryGroup.country} className="mb-1 last:mb-0">
                          <button
                            onClick={() =>
                              setExpandedCountry(expandedCountry === countryGroup.country ? null : countryGroup.country)
                            }
                            className="w-full px-4 py-2.5 flex items-center justify-between gap-2 hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-150"
                          >
                            <div className="flex items-center gap-2">
                              {countryGroup.countryLogoUrl ? (
                                <img
                                  src={countryGroup.countryLogoUrl}
                                  alt=""
                                  className="h-4 w-4 rounded-sm object-cover"
                                />
                              ) : countryGroup.countryFlag ? (
                                <span className="text-lg">{countryGroup.countryFlag}</span>
                              ) : (
                                <Globe className="h-4 w-4 text-muted-foreground" />
                              )}
                              <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                                {countryGroup.country}
                              </span>
                            </div>
                            <ChevronRight
                              className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ${expandedCountry === countryGroup.country ? "rotate-90" : ""}`}
                            />
                          </button>

                          <div
                            className={`overflow-hidden transition-all duration-200 ${expandedCountry === countryGroup.country ? "max-h-96 opacity-100" : "max-h-0 opacity-0"}`}
                          >
                            {countryGroup.leagues.map((league) => (
                              <button
                                key={league.id}
                                onClick={() => handleLeagueSelect(league)}
                                className={`w-full flex items-center gap-3 px-4 pl-10 py-2.5 text-sm text-left transition-all duration-150 ${
                                  currentLeague.id === league.id
                                    ? "bg-primary/10 dark:bg-primary/15 text-primary border-l-2 border-primary"
                                    : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 hover:pl-11"
                                }`}
                              >
                                <span className="flex h-6 w-6 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
                                  <img
                                    src={league.logo || "/placeholder.svg"}
                                    alt=""
                                    className="h-5 w-5 object-contain"
                                  />
                                </span>
                                <span className="font-medium">{league.name}</span>
                              </button>
                            ))}
                          </div>
                        </div>
                      ))}
                      {navLoading && leagueGroups.length === 0 && (
                        <div className="px-4 py-3 text-xs text-muted-foreground">Loading leagues...</div>
                      )}
                      {navError && (
                        <div className="px-4 py-2 text-xs text-amber-600 dark:text-amber-400">
                          Using fallback league navigation
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>

              <Link
                href="/explore"
                className="hidden sm:flex group items-center gap-1.5 px-3 py-2 rounded-xl text-xs font-medium text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-200 min-h-[44px]"
                aria-label="Explore all streaks"
              >
                <TrendingUp className="w-3.5 h-3.5 text-primary transition-transform group-hover:scale-110" aria-hidden="true" />
                <span>Explore</span>
              </Link>

              <Link href="/forecast-record" className="hidden sm:flex items-center px-3 py-2 text-xs font-medium min-h-[44px] rounded-xl hover:bg-secondary/60">Forecast record</Link>
              <div className="hidden sm:flex items-center gap-2">
                <SignedOut>
                  <SignInButton mode="redirect">
                    <button
                      type="button"
                      className="px-3 py-2 rounded-xl text-xs font-medium text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-200 min-h-[44px]"
                    >
                      Sign in
                    </button>
                  </SignInButton>
                  <SignUpButton mode="redirect">
                    <button
                      type="button"
                      className="px-3 py-2 rounded-xl text-xs font-medium bg-primary text-primary-foreground hover:opacity-90 transition-all duration-200 min-h-[44px]"
                    >
                      Sign up
                    </button>
                  </SignUpButton>
                </SignedOut>
                <SignedIn>
                  <Link
                    href="/dashboard"
                    className="px-3 py-2 rounded-xl text-xs font-medium text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-200 min-h-[44px] flex items-center"
                  >
                    Dashboard
                  </Link>
                  <UserButton afterSignOutUrl="/welcome?signed_out=1" />
                </SignedIn>
              </div>

              {/* Theme Toggle */}
              <ThemeToggle />
            </div>
          </div>

          {mobileMenuOpen && (
            <div className="md:hidden absolute top-full left-0 right-0 mt-2 mx-2 rounded-xl bg-popover/98 backdrop-blur-xl border border-border/60 shadow-xl shadow-black/10 dark:shadow-black/30 overflow-hidden z-[100]">
              <Link href="/forecast-record" className="block px-4 py-3 text-sm font-medium border-b border-border/40">Forecast record</Link>
              <button
                onClick={() => handleLeagueSelect(allLeaguesOption)}
                className={`w-full flex items-center gap-3 px-4 py-3 text-sm text-left transition-all duration-150 border-b border-border/40 ${
                  currentLeague.id === allLeaguesOption.id
                    ? "bg-primary/10 dark:bg-primary/20 text-primary"
                    : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40"
                }`}
              >
                <div className="flex items-center justify-center w-6 h-6 rounded-lg bg-primary/10 dark:bg-primary/20 border border-primary/20">
                  <Globe className="h-4 w-4 text-primary" />
                </div>
                <span className="font-medium">{allLeaguesOption.name}</span>
              </button>

              <div className="py-2 max-h-96 overflow-y-auto">
                {leagueGroups.map((countryGroup) => (
                  <div key={countryGroup.country} className="mb-1 last:mb-0">
                    <button
                      onClick={() =>
                        setExpandedCountry(expandedCountry === countryGroup.country ? null : countryGroup.country)
                      }
                      className="w-full px-4 py-2.5 flex items-center justify-between gap-2 hover:bg-secondary/60 dark:hover:bg-secondary/40 transition-all duration-150"
                    >
                      <div className="flex items-center gap-2">
                        {countryGroup.countryLogoUrl ? (
                          <img src={countryGroup.countryLogoUrl} alt="" className="h-4 w-4 rounded-sm object-cover" />
                        ) : countryGroup.countryFlag ? (
                          <span className="text-lg">{countryGroup.countryFlag}</span>
                        ) : (
                          <Globe className="h-4 w-4 text-muted-foreground" />
                        )}
                        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                          {countryGroup.country}
                        </span>
                      </div>
                      <ChevronRight
                        className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ${expandedCountry === countryGroup.country ? "rotate-90" : ""}`}
                      />
                    </button>

                    <div
                      className={`overflow-hidden transition-all duration-200 ${expandedCountry === countryGroup.country ? "max-h-96 opacity-100" : "max-h-0 opacity-0"}`}
                    >
                      {countryGroup.leagues.map((league) => (
                        <button
                          key={league.id}
                          onClick={() => handleLeagueSelect(league)}
                          className={`w-full flex items-center gap-3 px-4 pl-10 py-2.5 text-sm text-left transition-all duration-150 ${
                            currentLeague.id === league.id
                              ? "bg-primary/10 dark:bg-primary/15 text-primary border-l-2 border-primary"
                              : "text-foreground hover:bg-secondary/60 dark:hover:bg-secondary/40 hover:pl-11"
                          }`}
                        >
                          <span className="flex h-6 w-6 items-center justify-center rounded-md bg-white/95 p-0.5 shadow-sm ring-1 ring-black/5">
                            <img src={league.logo || "/placeholder.svg"} alt="" className="h-5 w-5 object-contain" />
                          </span>
                          <span className="font-medium">{league.name}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
                {navLoading && leagueGroups.length === 0 && (
                  <div className="px-4 py-3 text-xs text-muted-foreground">Loading leagues...</div>
                )}
                {navError && (
                  <div className="px-4 py-2 text-xs text-amber-600 dark:text-amber-400">
                    Using fallback league navigation
                  </div>
                )}
              </div>

              {/* Mobile Theme Toggle */}
              <div className="border-t border-border/40 p-4">
                <div className="flex flex-col gap-3">
                  <div className="flex items-center gap-2">
                    <SignedOut>
                      <SignInButton mode="redirect">
                        <button
                          type="button"
                          className="flex-1 px-3 py-2 rounded-lg text-sm font-medium border border-border/60 hover:bg-secondary/60"
                        >
                          Sign in
                        </button>
                      </SignInButton>
                      <SignUpButton mode="redirect">
                        <button
                          type="button"
                          className="flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-primary text-primary-foreground"
                        >
                          Sign up
                        </button>
                      </SignUpButton>
                    </SignedOut>
                    <SignedIn>
                      <Link
                        href="/dashboard"
                        onClick={() => setMobileMenuOpen(false)}
                        className="flex-1 px-3 py-2 rounded-lg text-sm font-medium border border-border/60 hover:bg-secondary/60 text-center"
                      >
                        Dashboard
                      </Link>
                      <UserButton afterSignOutUrl="/welcome?signed_out=1" />
                    </SignedIn>
                  </div>

                  <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-foreground">Theme</span>
                  <ThemeToggle />
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </nav>
  )
}
