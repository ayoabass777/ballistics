"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { ChevronLeft, ArrowLeft, TrendingUp } from "lucide-react"
import type { TeamPageViewModel } from "@/types/team-page"
import { TeamHeader } from "./TeamHeader"
import { UpcomingMatchBadge } from "./UpcomingMatchBadge"
import { CurrentStreaks } from "./CurrentStreaks"
import { ThemeToggle } from "@/components/theme-toggle"

type TeamPageShellProps = {
  viewModel: TeamPageViewModel
  returnTo?: string | null
}

export function TeamPageShell({ viewModel, returnTo }: TeamPageShellProps) {
  const router = useRouter()

  const teamSlug = viewModel.team.slug || viewModel.team.name.toLowerCase().replace(/\s+/g, "-")

  const handleBack = () => {
    if (returnTo && returnTo.length > 0) {
      router.push(returnTo)
    } else {
      if (typeof window !== "undefined" && window.history.length > 1) {
        router.back()
      } else {
        router.push("/")
      }
    }
  }

  const getBackLabel = () => {
    if (!returnTo) return "Back"
    if (returnTo.startsWith("/fixture/")) return "Back to Fixture"
    if (returnTo.startsWith("/explore")) return "Back to Explore"
    if (returnTo.startsWith("/league")) return "Back to League"
    if (returnTo === "/") return "Back to Home"
    return "Back"
  }

  return (
    <div className="min-h-screen font-sans text-foreground bg-background">
      {/* Header */}
      <header className="sticky top-0 z-50 w-full border-b border-border/40">
        <div className="absolute inset-0 bg-background/80 backdrop-blur-xl" />
        <div className="absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-primary/30 to-transparent" />
        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 py-3 flex items-center justify-between">
          {/* Back button */}
          <button
            onClick={handleBack}
            className="flex items-center gap-2.5 hover:opacity-70 transition-all group"
          >
            <div className="w-10 h-10 rounded-xl bg-primary/10 dark:bg-primary/20 border border-primary/20 dark:border-primary/30 flex items-center justify-center group-hover:border-primary/40 transition-all">
              <ArrowLeft className="w-5 h-5 text-primary" />
            </div>
            <span className="font-bold text-base text-foreground hidden sm:block">{getBackLabel()}</span>
          </button>

          {/* Right actions */}
          <div className="flex items-center gap-2 sm:gap-3">
            <Link
              href={viewModel.ctas.backToLeagueHref}
              className="group flex items-center gap-2 px-3 sm:px-4 py-2 rounded-lg text-xs sm:text-sm font-medium text-foreground bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-primary/30 transition-all duration-200"
            >
              <span className="hidden sm:inline">League Table</span>
              <span className="sm:hidden">Table</span>
            </Link>
            <Link
              href="/explore"
              className="group flex items-center gap-2 px-3 sm:px-4 py-2 rounded-lg text-xs sm:text-sm font-medium text-foreground bg-secondary/60 dark:bg-secondary/40 hover:bg-secondary/80 dark:hover:bg-secondary/60 border border-border/50 hover:border-primary/30 transition-all duration-200"
            >
              <TrendingUp className="w-3.5 h-3.5 text-primary" />
              <span className="hidden sm:inline">Explore</span>
            </Link>
            <ThemeToggle />
          </div>
        </div>
      </header>

      {/* Main content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 sm:py-8">
        {/* Team Header */}
        <TeamHeader
          team={viewModel.team}
          context={viewModel.context}
          backToLeagueHref={viewModel.ctas.backToLeagueHref}
          exploreStreaksHref={viewModel.ctas.exploreStreaksHref}
          formLastN={viewModel.form?.lastN}
        />

        {/* Upcoming Match Badge (optional) */}
        {viewModel.uiHints?.showUpcomingMatch && viewModel.upcomingMatch && (
          <UpcomingMatchBadge
            upcomingMatch={viewModel.upcomingMatch}
            teamSlug={teamSlug}
          />
        )}

        {/* Current Streaks */}
        <CurrentStreaks
          currentStreaks={viewModel.currentStreaks}
          team={viewModel.team}
          showSecondary={viewModel.uiHints?.showSecondaryStreakRow ?? false}
        />

        {/* Footer CTAs */}
        <div className="mt-16 pt-10 border-t border-border/30">
          <div className="flex items-center justify-center">
            <Link
              href={viewModel.ctas.backToLeagueHref}
              className="group flex items-center gap-3 px-6 py-4 rounded-xl glass-panel hover:border-primary/30 transition-all duration-300 hover:scale-[1.02]"
            >
              <ChevronLeft className="w-5 h-5 text-muted-foreground group-hover:text-primary transition-colors" />
              <span className="text-sm font-medium text-muted-foreground group-hover:text-foreground transition-colors">
                Back to {viewModel.context.leagueName}
              </span>
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}
