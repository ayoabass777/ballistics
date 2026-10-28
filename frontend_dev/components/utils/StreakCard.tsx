"use client"

import { Home, Plane, Activity, ArrowRight } from "lucide-react"
import { MatchCountdown } from "@/components/ui/MatchCountdown"

interface StreakCardProps {
  teamName: string
  teamLogoUrl?: string
  leagueName: string
  countryName?: string
  countryLogoUrl?: string
  leagueLogoUrl?: string
  // Primary headline streak (narrative style)
  headline: string
  // Optional secondary streak (same polarity, reinforcing)
  secondaryStreak?: string
  // Recent form as array of 'W' | 'D' | 'L' — omit to hide the row
  recentForm?: Array<"W" | "D" | "L">
  scope?: "overall" | "home" | "away"
  nextOpponent?: string
  nextIsHome?: boolean
  nextMatchDate?: string
  onViewStats: () => void
  index: number
  // Bayesian confidence score [0, 1] — drives the HOT / WARM / COOLING badge
  confidenceScore?: number
  // Current team Elo tier: 1=Bottom, 2=Mid, 3=Top
  teamTier?: number
  // Opponent Elo tier for the next fixture: 1=Weak, 2=Mid, 3=Strong
  opponentTier?: number
}


export function generateHeadline(eventName: string, streakLength: number): string {
  if (!eventName) return "Active streak"
  if (!streakLength || streakLength < 1) return eventName
  return `${streakLength}-match ${eventName.toLowerCase()} streak`
}

// ---------------------------------------------------------------------------
// Confidence badge — derived from Bayesian final_score
// ---------------------------------------------------------------------------

type ConfidenceLevel = "HOT" | "WARM" | "COOLING"

function getConfidenceLevel(score: number): ConfidenceLevel {
  if (score >= 0.75) return "HOT"
  if (score >= 0.55) return "WARM"
  return "COOLING"
}

const CONFIDENCE_CONFIG: Record<
  ConfidenceLevel,
  { label: string; dot: string; pill: string; text: string; tooltip: string }
> = {
  HOT: {
    label: "HOT",
    dot: "bg-emerald-500",
    pill: "bg-emerald-50 dark:bg-emerald-950/50 border-emerald-200/60 dark:border-emerald-700/40",
    text: "text-emerald-700 dark:text-emerald-400",
    tooltip: "Streak is statistically reliable — form has been consistent.",
  },
  WARM: {
    label: "WARM",
    dot: "bg-amber-500",
    pill: "bg-amber-50 dark:bg-amber-950/50 border-amber-200/60 dark:border-amber-700/40",
    text: "text-amber-700 dark:text-amber-400",
    tooltip: "Streak is holding — form is decent but not dominant.",
  },
  COOLING: {
    label: "COOLING",
    dot: "bg-rose-500",
    pill: "bg-rose-50 dark:bg-rose-950/50 border-rose-200/60 dark:border-rose-700/40",
    text: "text-rose-700 dark:text-rose-400",
    tooltip: "Streak may be losing momentum — form has been inconsistent.",
  },
}

function ConfidenceBadge({ score }: { score: number }) {
  const level = getConfidenceLevel(score)
  const cfg = CONFIDENCE_CONFIG[level]

  return (
    <div className="relative group/conf">
      <div
        title={cfg.tooltip}
        className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border text-[10px] font-bold tracking-wide ${cfg.pill}`}
      >
        <span className={`h-1.5 w-1.5 rounded-full flex-shrink-0 ${cfg.dot}`} />
        <span className={cfg.text}>{cfg.label}</span>
      </div>
      <div
        role="tooltip"
        className="pointer-events-none absolute left-0 top-full z-20 mt-1 w-52 rounded-md border border-border/70 bg-popover px-2.5 py-1.5 text-[11px] leading-snug text-popover-foreground shadow-md opacity-0 transition-opacity duration-150 group-hover/conf:opacity-100"
      >
        {cfg.tooltip}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Opponent rank tag — derived from opponent Elo tier (1=Bottom, 2=Mid, 3=Top)
// Uses football table-position language: "playing a top side" / "bottom side".
// ---------------------------------------------------------------------------

const TIER_CONFIG: Record<number, { label: string; pill: string; text: string; tooltip: string }> = {
  1: {
    label: "Bottom",
    pill: "bg-rose-50 dark:bg-rose-950/40 border-rose-200/50 dark:border-rose-700/30",
    text: "text-rose-700 dark:text-rose-400",
    tooltip: "Next opponent is a bottom-tier side — favourable matchup.",
  },
  2: {
    label: "Mid",
    pill: "bg-amber-50 dark:bg-amber-950/40 border-amber-200/50 dark:border-amber-700/30",
    text: "text-amber-700 dark:text-amber-400",
    tooltip: "Next opponent is a mid-table side — balanced matchup.",
  },
  3: {
    label: "Top",
    pill: "bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200/50 dark:border-emerald-700/30",
    text: "text-emerald-700 dark:text-emerald-400",
    tooltip: "Next opponent is a top-tier side — tougher matchup.",
  },
}

function OpponentDifficultyTag({ tier }: { tier: number }) {
  const cfg = TIER_CONFIG[tier]
  if (!cfg) return null

  return (
    <div className="relative group/tier">
      <span
        title={cfg.tooltip}
        className={`inline-flex items-center px-1.5 py-0.5 rounded border text-[10px] font-semibold ${cfg.pill} ${cfg.text}`}
      >
        {cfg.label}
      </span>
      <div
        role="tooltip"
        className="pointer-events-none absolute right-0 top-full z-20 mt-1 w-52 rounded-md border border-border/70 bg-popover px-2.5 py-1.5 text-[11px] leading-snug text-popover-foreground shadow-md opacity-0 transition-opacity duration-150 group-hover/tier:opacity-100"
      >
        {cfg.tooltip}
      </div>
    </div>
  )
}

function TeamTierTag({ tier }: { tier: number }) {
  const cfg = TIER_CONFIG[tier]
  if (!cfg) return null

  return (
    <div className="relative group/team-tier">
      <span
        title={`Current team is ${cfg.label.toLowerCase()} tier based on team strength.`}
        className={`inline-flex items-center px-1.5 py-0.5 rounded border text-[10px] font-semibold ${cfg.pill} ${cfg.text}`}
      >
        {cfg.label}
      </span>
      <div
        role="tooltip"
        className="pointer-events-none absolute right-0 top-full z-20 mt-1 w-52 rounded-md border border-border/70 bg-popover px-2.5 py-1.5 text-[11px] leading-snug text-popover-foreground shadow-md opacity-0 transition-opacity duration-150 group-hover/team-tier:opacity-100"
      >
        How this team’s strength compares with others in its league.
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Form dots
// ---------------------------------------------------------------------------

function FormDot({ result, index }: { result: "W" | "D" | "L"; index: number }) {
  const baseStyles = "h-2.5 w-2.5 rounded-full transition-all duration-200"
  const colorMap = {
    W: "bg-emerald-500 dark:bg-emerald-400",
    D: "bg-amber-500 dark:bg-amber-400",
    L: "bg-rose-500 dark:bg-rose-400",
  }
  const labelMap = { W: "Win", D: "Draw", L: "Loss" }

  return (
    <div
      className={`${baseStyles} ${colorMap[result]}`}
      title={labelMap[result]}
      style={{ animationDelay: `${index * 50}ms` }}
    />
  )
}

// ---------------------------------------------------------------------------
// Context badge
// ---------------------------------------------------------------------------

const CONTEXT_CONFIG = {
  overall: {
    icon: Activity,
    label: "Overall",
    containerClass: "bg-secondary/80 dark:bg-secondary/60 border-border/50",
    iconClass: "text-muted-foreground",
    textClass: "text-muted-foreground",
  },
  home: {
    icon: Home,
    label: "Home",
    containerClass: "bg-blue-50 dark:bg-blue-950/40 border-blue-200/50 dark:border-blue-800/40",
    iconClass: "text-blue-600 dark:text-blue-400",
    textClass: "text-blue-700 dark:text-blue-300",
  },
  away: {
    icon: Plane,
    label: "Away",
    containerClass: "bg-purple-50 dark:bg-purple-950/40 border-purple-200/50 dark:border-purple-800/40",
    iconClass: "text-purple-600 dark:text-purple-400",
    textClass: "text-purple-700 dark:text-purple-300",
  },
}

function ContextBadge({ scope }: { scope?: "overall" | "home" | "away" }) {
  if (!scope) return null
  const context = CONTEXT_CONFIG[scope]
  const ContextIcon = context.icon
  const tooltipText =
    scope === "home"
      ? "Context-based streak: home matches only."
      : scope === "away"
        ? "Context-based streak: away matches only."
        : "Context-based streak: combines home and away matches."

  return (
    <div className="relative group/context">
      <button
        type="button"
        title={tooltipText}
        aria-label={tooltipText}
        className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border ${context.containerClass} focus:outline-none`}
      >
        <ContextIcon className={`w-3 h-3 ${context.iconClass}`} />
        <span className={`text-xs font-medium ${context.textClass}`}>{context.label}</span>
      </button>
      <div
        role="tooltip"
        className="pointer-events-none absolute right-0 top-full z-20 mt-1 w-56 rounded-md border border-border/70 bg-popover px-2.5 py-1.5 text-[11px] leading-snug text-popover-foreground shadow-md opacity-0 transition-opacity duration-150 group-hover/context:opacity-100 group-focus-within/context:opacity-100"
      >
        {tooltipText}
      </div>
    </div>
  )
}

function getContextHint(scope: "overall" | "home" | "away"): string {
  if (scope === "home") return "Context: home matches only"
  if (scope === "away") return "Context: away matches only"
  return "Context: home and away matches"
}

// ---------------------------------------------------------------------------
// Main card
// ---------------------------------------------------------------------------

export function StreakCard({
  teamName,
  teamLogoUrl,
  leagueName,
  countryName,
  countryLogoUrl,
  leagueLogoUrl,
  headline,
  secondaryStreak,
  recentForm,
  scope,
  nextOpponent,
  nextIsHome,
  nextMatchDate,
  onViewStats,
  index,
  confidenceScore,
  teamTier,
  opponentTier,
}: StreakCardProps) {
  return (
    <div
      className="glass-panel streak-card-glow rounded-2xl h-full flex flex-col animate-fade-in group overflow-hidden"
      style={{ animationDelay: `${index * 40}ms` }}
    >
      {/* Accent line at top */}
      <div className="streak-card-accent" />

      <div className="p-5 sm:p-6 flex flex-col gap-4 flex-1">
        {/* Header: Team + badge stack */}
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-3 flex-1 min-w-0">
            <div className="relative flex-shrink-0">
              <div className="h-12 w-12 sm:h-14 sm:w-14 rounded-xl bg-secondary/50 dark:bg-secondary/30 p-1.5 ring-1 ring-border/50 group-hover:ring-primary/30 transition-all duration-300">
                <div className="h-full w-full rounded-lg bg-white/95 dark:bg-white p-1 shadow-sm ring-1 ring-black/5">
                  <img
                    src={teamLogoUrl || "/placeholder.svg"}
                    alt={teamName}
                    className="h-full w-full object-contain"
                  />
                </div>
              </div>
            </div>
            <div className="flex-1 min-w-0">
              <h3 className="text-base sm:text-lg font-bold text-foreground leading-tight truncate">{teamName}</h3>
              <div className="flex items-center gap-1.5 mt-0.5">
                {leagueLogoUrl && (
                  <img src={leagueLogoUrl} className="h-3.5 w-3.5 object-contain flex-shrink-0 opacity-80" alt="" />
                )}
                <span className="text-xs text-muted-foreground truncate">{leagueName}</span>
              </div>
            </div>
          </div>
          <div className="flex flex-col items-end gap-2 shrink-0">
            {scope && <ContextBadge scope={scope} />}
            {typeof teamTier === "number" && <TeamTierTag tier={teamTier} />}
          </div>
        </div>

        {/* Streak Headline - Hero element */}
        <div className="relative bg-gradient-to-br from-primary/8 via-primary/5 to-transparent dark:from-primary/15 dark:via-primary/8 dark:to-transparent border border-primary/10 dark:border-primary/20 rounded-xl p-4 group-hover:border-primary/25 dark:group-hover:border-primary/35 transition-colors">
          {/* Confidence badge pinned to top-right of headline block */}
          {typeof confidenceScore === "number" && (
            <div className="absolute top-3 right-3">
              <ConfidenceBadge score={confidenceScore} />
            </div>
          )}
          <p className="text-lg sm:text-xl font-bold text-foreground leading-snug pr-20">{headline}</p>
          {scope && (
            <p className="text-[11px] text-muted-foreground mt-1">{getContextHint(scope)}</p>
          )}
          {secondaryStreak && (
            <p className="text-sm text-muted-foreground mt-1.5 font-medium">{secondaryStreak}</p>
          )}
        </div>

        {/* Recent Form - Compact (hidden when data is unavailable) */}
        {recentForm && recentForm.length > 0 && (
          <div className="flex items-center gap-3">
            <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider">Form</span>
            <div className="flex items-center gap-1.5">
              {recentForm.slice(0, 5).map((res, idx) => (
                <FormDot key={`${res}-${idx}`} result={res} index={idx} />
              ))}
            </div>
          </div>
        )}

        {/* Spacer to push footer to bottom */}
        <div className="flex-1" />

        {/* Footer: Next Match + CTA */}
        <div className="pt-3 border-t border-border/40 dark:border-border/30 space-y-3">
          {/* Next Match Info */}
          <div className="flex items-center justify-between gap-2">
            {(countryLogoUrl || countryName) ? (
              <div className="flex items-center gap-2 min-w-0">
                {countryLogoUrl && (
                  <img src={countryLogoUrl} alt="" className="h-4 w-4 object-contain flex-shrink-0 opacity-75" />
                )}
                {countryName && (
                  <span className="text-xs text-muted-foreground truncate">{countryName}</span>
                )}
              </div>
            ) : (
              <div />
            )}
            <div className="flex items-center gap-2 text-right shrink-0">
              <div>
                {nextOpponent && (
                  <p className="text-sm font-semibold text-foreground inline-flex items-center gap-1.5">
                    {typeof nextIsHome === "boolean" &&
                      (nextIsHome ? (
                        <Home className="h-3.5 w-3.5 text-muted-foreground" aria-label="Home match" />
                      ) : (
                        <Plane className="h-3.5 w-3.5 text-muted-foreground" aria-label="Away match" />
                      ))}
                    <span>vs {nextOpponent}</span>
                    {/* Opponent difficulty — shown only when tier is provided */}
                    {typeof opponentTier === "number" && (
                      <OpponentDifficultyTag tier={opponentTier} />
                    )}
                  </p>
                )}
                {nextMatchDate && (
                  <MatchCountdown matchDate={nextMatchDate} className="justify-end" />
                )}
              </div>
            </div>
          </div>

          {/* CTA Button */}
          <button
            onClick={onViewStats}
            className="w-full inline-flex items-center justify-center gap-2 px-4 py-3 rounded-xl text-sm font-semibold bg-primary text-primary-foreground hover:bg-primary/90 active:scale-[0.98] transition-all duration-200 group/btn"
          >
            <span>View Analysis</span>
            <ArrowRight className="h-4 w-4 transition-transform group-hover/btn:translate-x-0.5" />
          </button>
        </div>
      </div>
    </div>
  )
}
