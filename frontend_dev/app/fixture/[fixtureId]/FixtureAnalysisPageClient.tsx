"use client"

import { useRouter } from "next/navigation"
import { TrendingUp, Share2, ChevronDown } from "lucide-react"
import { useState } from "react"
import { FixtureHeader } from "@/components/fixture/FixtureHeader"
import { FixtureMatchupHero } from "@/components/fixture/FixtureMatchupHero"
import { TeamAvgGoals } from "@/components/fixture/TeamAvgGoals"
import { StreakItem } from "@/components/fixture/StreakItem"
import { H2HOverview } from "@/components/fixture/H2HOverview"
import { H2HStreakItem } from "@/components/fixture/H2HStreakItem"
import { RecentMeetings } from "@/components/fixture/RecentMeetings"
import { FixturePageSkeleton } from "@/components/fixture/FixturePageSkeleton"
import { useToast } from "@/components/Toast"
import { useFixtureData } from "@/lib/hooks/useFixtureData"

interface FixtureAnalysisPageClientProps {
  fixtureId: string
  returnTo?: string | null
}

export function FixtureAnalysisPageClient({ fixtureId, returnTo }: FixtureAnalysisPageClientProps) {
  const router = useRouter()
  const { showToast } = useToast()
  const { fixture, loading, error, status } = useFixtureData(fixtureId)
  const [showAllTeamForm, setShowAllTeamForm] = useState(false)
  const currentFixtureHref = `/fixture/${fixtureId}${returnTo ? `?returnTo=${encodeURIComponent(returnTo)}` : ""}`

  if (loading || status === "loading") {
    return (
      <div className="min-h-screen">
        <FixtureHeader returnTo={returnTo} />
        <FixturePageSkeleton />
      </div>
    )
  }

  if (status === "not_found") {
    return (
      <div className="min-h-screen">
        <FixtureHeader returnTo={returnTo} />
        <div className="flex items-center justify-center min-h-[60vh]">
          <div className="text-center space-y-4 px-4 max-w-lg">
            <div className="w-16 h-16 rounded-full bg-amber-500/10 border border-amber-500/20 flex items-center justify-center mx-auto">
              <svg
                className="w-8 h-8 text-amber-400"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-foreground">Fixture analysis not available yet</h2>
            <p className="text-muted-foreground">
              We found the fixture route, but the full analysis dataset for this fixture is not available yet.
            </p>
            <p className="text-xs text-muted-foreground/80 font-mono">fixture_id: {fixtureId}</p>
            <button
              onClick={() => (returnTo ? router.push(returnTo) : router.push("/"))}
              className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-blue-500 hover:bg-blue-600 text-white font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
            >
              Back
            </button>
          </div>
        </div>
      </div>
    )
  }

  if (status === "error" || error || !fixture) {
    return (
      <div className="min-h-screen">
        <FixtureHeader returnTo={returnTo} />
        <div className="flex items-center justify-center min-h-[60vh]">
          <div className="text-center space-y-4 px-4">
            <div className="w-16 h-16 rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center mx-auto">
              <svg
                className="w-8 h-8 text-red-400"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-foreground">Failed to load fixture</h2>
            <p className="text-muted-foreground max-w-md mx-auto">{error}</p>
            <button
              onClick={() => router.push("/")}
              className="inline-flex items-center gap-2 px-6 py-3 rounded-xl bg-blue-500 hover:bg-blue-600 text-white font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
            >
              Back to Home
            </button>
          </div>
        </div>
      </div>
    )
  }

  const sortedTeamStreaks = [
    ...fixture.homeStreaks.map((streak) => ({ streak, team: fixture.homeTeam })),
    ...fixture.awayStreaks.map((streak) => ({ streak, team: fixture.awayTeam })),
  ].sort((a, b) => b.streak.count - a.streak.count)
  const primaryTeamStreaks = sortedTeamStreaks.slice(0, 5)
  const additionalTeamStreaks = sortedTeamStreaks.slice(5)

  return (
    <div className="min-h-screen">
      <FixtureHeader returnTo={returnTo} />

      <main className="pb-16 px-4 sm:px-6 lg:px-8">
        <div className="max-w-6xl mx-auto space-y-6 pt-6">
          <FixtureMatchupHero
            fixture={{
              ...fixture,
              homeTeamHref: `/team/${fixture.homeTeam.slug}?returnTo=${encodeURIComponent(currentFixtureHref)}`,
              awayTeamHref: `/team/${fixture.awayTeam.slug}?returnTo=${encodeURIComponent(currentFixtureHref)}`,
            }}
          />

          {/* Team Form Section */}
          <div className="space-y-5">
            <div className="flex flex-col items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500/20 to-cyan-500/10 border border-blue-500/20 flex items-center justify-center">
                <TrendingUp className="h-5 w-5 text-blue-400" />
              </div>
              <div className="text-center">
                <h3 className="text-xl sm:text-2xl font-bold text-foreground">Team Form</h3>
                <p className="text-sm text-muted-foreground">Current patterns heading into this fixture</p>
              </div>
            </div>

            <div className="grid gap-3.5">
              {primaryTeamStreaks.map((item, idx) => (
                <StreakItem
                  key={`${item.team.teamId}-${item.streak.event}-${item.streak.scope}-${item.streak.count}-${idx}`}
                  streak={item.streak}
                  team={item.team}
                  index={idx}
                />
              ))}
            </div>

            {additionalTeamStreaks.length > 0 && (
              <div className="space-y-3">
                <button
                  type="button"
                  onClick={() => setShowAllTeamForm((prev) => !prev)}
                  className="w-full sm:w-auto inline-flex items-center justify-center gap-2 px-4 py-2 rounded-lg bg-background/40 border border-border/40 hover:bg-background/60 transition-all duration-200 text-sm font-medium text-foreground"
                  aria-expanded={showAllTeamForm}
                  aria-controls="fixture-team-form-extra"
                >
                  <span>{showAllTeamForm ? "Show top 5 only" : `Show ${additionalTeamStreaks.length} more`}</span>
                  <ChevronDown
                    className={`h-4 w-4 transition-transform duration-200 ${showAllTeamForm ? "rotate-180" : ""}`}
                  />
                </button>

                {showAllTeamForm && (
                  <div id="fixture-team-form-extra" className="grid gap-3.5">
                    {additionalTeamStreaks.map((item, idx) => (
                      <StreakItem
                        key={`${item.team.teamId}-${item.streak.event}-${item.streak.scope}-${item.streak.count}-${idx + 5}`}
                        streak={item.streak}
                        team={item.team}
                        index={idx + 5}
                      />
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* H2H Section */}
          <div className="space-y-5">
            <H2HOverview h2hStats={fixture.h2hStats} homeTeam={fixture.homeTeam} awayTeam={fixture.awayTeam} />

            <div className="grid gap-3.5">
              {fixture.h2hStats.streaks.map((streak, idx) => (
                <H2HStreakItem
                  key={idx}
                  streak={streak}
                  index={idx}
                  homeTeam={fixture.homeTeam}
                  awayTeam={fixture.awayTeam}
                />
              ))}
            </div>
          </div>

          {/* Recent Meetings */}
          <RecentMeetings
            recentMatches={fixture.h2hStats.recentMatches}
            homeTeamForm={fixture.homeTeamForm}
            awayTeamForm={fixture.awayTeamForm}
            homeTeam={{
              name: fixture.homeTeam.name,
              logoUrl: fixture.homeTeam.logoUrl,
            }}
            awayTeam={{
              name: fixture.awayTeam.name,
              logoUrl: fixture.awayTeam.logoUrl,
            }}
          />

          {/* Average Goals Section - at the end */}
          <TeamAvgGoals homeTeam={fixture.homeTeam} awayTeam={fixture.awayTeam} />
        </div>
      </main>

      <button
        className="fixed bottom-6 left-6 z-50 w-14 h-14 rounded-full bg-gradient-to-r from-muted/80 to-muted/60 backdrop-blur-md border border-border/40 shadow-lg hover:shadow-xl hover:scale-110 transition-all duration-200 flex items-center justify-center group focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
        onClick={async () => {
          if (navigator.share) {
            try {
              await navigator.share({
                title: `${fixture.homeTeam.name} vs ${fixture.awayTeam.name}`,
                text: "Check out this fixture analysis on Ballistics",
                url: window.location.href,
              })
              showToast("Shared successfully!", "success")
            } catch (err: any) {
              if (err.name !== "AbortError") {
                console.error("Share error:", err)
                showToast("Failed to share", "error")
              }
            }
          } else {
            try {
              await navigator.clipboard.writeText(window.location.href)
              showToast("Link copied to clipboard!", "success")
            } catch (err) {
              console.error("Copy error:", err)
              showToast("Failed to copy link", "error")
            }
          }
        }}
        aria-label="Share fixture analysis"
      >
        <Share2 className="w-5 h-5 text-foreground group-hover:text-blue-400 transition-colors" />
      </button>
    </div>
  )
}
