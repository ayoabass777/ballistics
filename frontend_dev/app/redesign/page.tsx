"use client"

import Script from "next/script"
import { useEffect, useMemo, useState } from "react"
import {
  ArrowRight,
  ChevronDown,
  CircleUserRound,
  Clock3,
  Crosshair,
  Flame,
  Menu,
  Search,
  ShieldCheck,
  Sparkles,
  TrendingUp,
} from "lucide-react"

type FormResult = "W" | "D" | "L"

type Streak = {
  streak_id: number
  metric_key: string
  headline: string
  current_streak_length: number
  team_name: string
  league_name: string
  country_name?: string
  team_logo_url?: string
  league_logo_url?: string
  next_match_date?: string
  next_opponent?: string
  next_is_home?: boolean
  scope?: "overall" | "home" | "away"
  recent_form?: FormResult[]
}

const fallbackStreaks: Streak[] = [
  {
    streak_id: 1,
    metric_key: "unbeaten",
    headline: "Unbeaten in 16 straight matches",
    current_streak_length: 16,
    team_name: "Sirius",
    league_name: "Allsvenskan",
    country_name: "Sweden",
    next_opponent: "Mjallby AIF",
    next_is_home: false,
    scope: "overall",
    recent_form: ["D", "W", "W", "W", "W"],
  },
  {
    streak_id: 2,
    metric_key: "winless",
    headline: "Winless in 20 straight matches",
    current_streak_length: 20,
    team_name: "Chapecoense-SC",
    league_name: "Serie A",
    country_name: "Brazil",
    next_opponent: "Bahia",
    next_is_home: true,
    scope: "overall",
    recent_form: ["L", "D", "L", "L", "L"],
  },
]

const filters = ["All signals", "Unbeaten", "Goals", "Clean sheets"]

function formatKickoff(value?: string) {
  if (!value) return "Fixture pending"
  return new Intl.DateTimeFormat("en", {
    weekday: "short",
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value))
}

function signalTone(metric: string) {
  if (metric.includes("winless") || metric.includes("loss")) return "danger"
  if (metric.includes("unbeaten") || metric.includes("win") || metric.includes("clean")) return "positive"
  return "warning"
}

function confidenceFor(streak: Streak) {
  if (streak.current_streak_length >= 15) return "Elite"
  if (streak.current_streak_length >= 10) return "Strong"
  return "Watch"
}

function FormStrip({ form = [] }: { form?: FormResult[] }) {
  return (
    <div className="flex items-center gap-1" aria-label={`Recent form: ${form.join(", ")}`}>
      {form.slice(-5).map((result, index) => (
        <span
          key={`${result}-${index}`}
          className={`flex h-5 w-5 items-center justify-center rounded text-[10px] font-bold text-white ${
            result === "W" ? "bg-[#19875c]" : result === "D" ? "bg-[#d08a24]" : "bg-[#d65353]"
          }`}
        >
          {result}
        </span>
      ))}
    </div>
  )
}

function TeamMark({ streak }: { streak: Streak }) {
  return (
    <span className="flex h-11 w-11 shrink-0 items-center justify-center rounded-md border border-[#dce2dd] bg-white p-1.5">
      {streak.team_logo_url ? (
        <img src={streak.team_logo_url} alt="" className="h-full w-full object-contain" />
      ) : (
        <span className="text-xs font-bold text-[#175c43]">{streak.team_name.slice(0, 3).toUpperCase()}</span>
      )}
    </span>
  )
}

export default function RedesignPage() {
  const [streaks, setStreaks] = useState<Streak[]>(fallbackStreaks)
  const [activeFilter, setActiveFilter] = useState(filters[0])
  const [query, setQuery] = useState("")

  useEffect(() => {
    fetch("/api/streaks")
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error("Request failed"))))
      .then((data: Streak[]) => setStreaks(data))
      .catch(() => undefined)
  }, [])

  const ranked = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase()
    return streaks
      .filter((streak) => {
        if (activeFilter === "Unbeaten" && !streak.metric_key.includes("unbeaten")) return false
        if (activeFilter === "Goals" && !streak.metric_key.includes("goal")) return false
        if (activeFilter === "Clean sheets" && !streak.metric_key.includes("clean")) return false
        if (!normalizedQuery) return true
        return `${streak.team_name} ${streak.league_name}`.toLowerCase().includes(normalizedQuery)
      })
      .sort((a, b) => b.current_streak_length - a.current_streak_length)
      .slice(0, 7)
  }, [activeFilter, query, streaks])

  const breakingSoon = streaks
    .filter((streak) => streak.next_match_date)
    .sort((a, b) => new Date(a.next_match_date!).getTime() - new Date(b.next_match_date!).getTime())
    .slice(0, 3)

  return (
    <main className="min-h-screen bg-[#f4f6f2] font-sans text-[#151a17]">
      <Script src="https://mcp.figma.com/mcp/html-to-design/capture.js" strategy="afterInteractive" />

      <header className="border-b border-white/10 bg-[#10241b] text-white">
        <div className="mx-auto flex h-[72px] max-w-[1440px] items-center gap-7 px-4 sm:px-8 lg:px-16">
          <a href="/" className="flex shrink-0 items-center gap-2.5" aria-label="Ballistics home">
            <span className="flex h-9 w-9 items-center justify-center rounded-md bg-[#c7f36b] text-[#10241b]">
              <Crosshair className="h-5 w-5" />
            </span>
            <span className="hidden text-[15px] font-bold sm:block">BALLISTICS</span>
          </a>

          <nav className="hidden items-center gap-6 text-sm lg:flex" aria-label="Primary navigation">
            <a href="#board" className="font-semibold text-white">Live</a>
            <a href="/explore" className="text-white/65 transition-colors hover:text-white">Explore</a>
            <a href="/forecast-record" className="text-white/65 transition-colors hover:text-white">Forecast record</a>
            <button className="flex items-center gap-1 text-white/65 transition-colors hover:text-white">
              Leagues <ChevronDown className="h-3.5 w-3.5" />
            </button>
          </nav>

          <label className="relative ml-auto hidden w-full max-w-[330px] md:block">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-white/45" />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search team or league"
              className="h-11 w-full rounded-md border border-white/15 bg-white/8 pl-10 pr-4 text-sm text-white outline-none placeholder:text-white/45 focus:border-[#c7f36b]"
            />
          </label>

          <a href="/explore" className="hidden h-11 items-center gap-2 rounded-md bg-[#c7f36b] px-4 text-sm font-bold text-[#10241b] sm:flex">
            Explore all <ArrowRight className="h-4 w-4" />
          </a>
          <button className="ml-auto flex h-11 w-11 items-center justify-center rounded-md border border-white/15 md:hidden" aria-label="Open menu">
            <Menu className="h-5 w-5" />
          </button>
        </div>
      </header>

      <div className="mx-auto max-w-[1440px] px-4 py-8 sm:px-8 lg:px-16 lg:py-12">
        <section className="grid items-end gap-8 border-b border-[#dce2dd] pb-8 lg:grid-cols-[1fr_520px] lg:gap-14">
          <div>
            <div className="mb-4 inline-flex items-center gap-2 rounded-md bg-[#e1f2ea] px-2.5 py-1.5 text-[11px] font-bold text-[#175c43]">
              <span className="h-2 w-2 rounded-full bg-[#19875c]" />
              LIVE MARKET PULSE
            </div>
            <h1 className="max-w-2xl text-[34px] font-bold leading-[1.08] tracking-normal sm:text-[42px]">
              Football form, ranked by signal.
            </h1>
            <p className="mt-4 max-w-2xl text-sm leading-6 text-[#66706a] sm:text-base">
              Current streaks with opponent context, recent form and the next fixture that can break them.
            </p>
          </div>

          <div className="grid grid-cols-3 divide-x divide-[#dce2dd] rounded-md border border-[#dce2dd] bg-white">
            {[
              [streaks.length.toLocaleString(), "active streaks"],
              ["18", "leagues"],
              ["392", "teams tracked"],
            ].map(([value, label]) => (
              <div key={label} className="px-3 py-5 text-center sm:px-5">
                <p className="text-xl font-bold sm:text-[28px]">{value}</p>
                <p className="mt-1 text-[10px] text-[#66706a] sm:text-xs">{label}</p>
              </div>
            ))}
          </div>
        </section>

        <section id="board" className="pt-8">
          <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
            <div>
              <h2 className="text-2xl font-bold">Streak board</h2>
              <p className="mt-1 flex items-center gap-1.5 text-xs text-[#66706a]">
                <Clock3 className="h-3.5 w-3.5" /> Updated 2 minutes ago
              </p>
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <div className="flex max-w-full gap-2 overflow-x-auto pb-1 sm:pb-0" aria-label="Signal filters">
                {filters.map((filter) => (
                  <button
                    key={filter}
                    onClick={() => setActiveFilter(filter)}
                    className={`h-9 shrink-0 rounded-md border px-3 text-xs font-semibold transition-colors ${
                      activeFilter === filter
                        ? "border-[#175c43] bg-[#175c43] text-white"
                        : "border-[#dce2dd] bg-white text-[#465049] hover:border-[#175c43]"
                    }`}
                  >
                    {filter}
                  </button>
                ))}
              </div>
              <button className="flex h-9 items-center justify-between gap-4 rounded-md border border-[#dce2dd] bg-white px-3 text-xs font-semibold sm:min-w-32">
                All leagues <ChevronDown className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>

          <div className="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1fr)_380px]">
            <div className="overflow-hidden rounded-md border border-[#dce2dd] bg-white">
              <div className="hidden grid-cols-[36px_220px_minmax(230px,1fr)_104px_160px] items-center gap-4 bg-[#e1f2ea] px-5 py-3 text-[10px] font-bold text-[#175c43] md:grid">
                <span>#</span><span>TEAM</span><span>SIGNAL</span><span>STRENGTH</span><span>NEXT FIXTURE</span>
              </div>

              {ranked.length === 0 ? (
                <div className="px-5 py-16 text-center text-sm text-[#66706a]">No streaks match these filters.</div>
              ) : (
                ranked.map((streak, index) => {
                  const tone = signalTone(streak.metric_key)
                  return (
                    <article
                      key={streak.streak_id}
                      className="grid gap-4 border-t border-[#e7ebe8] p-4 first:border-t-0 md:grid-cols-[36px_220px_minmax(230px,1fr)_104px_160px] md:items-center md:px-5 md:py-4"
                    >
                      <span className="hidden text-sm font-bold text-[#7c847f] md:block">{index + 1}</span>
                      <div className="flex min-w-0 items-center gap-3">
                        <TeamMark streak={streak} />
                        <div className="min-w-0">
                          <h3 className="truncate text-sm font-semibold">{streak.team_name}</h3>
                          <p className="mt-0.5 truncate text-[11px] text-[#66706a]">{streak.league_name} · {streak.country_name}</p>
                        </div>
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className={`h-2 w-2 shrink-0 rounded-full ${tone === "positive" ? "bg-[#19875c]" : tone === "danger" ? "bg-[#d65353]" : "bg-[#d08a24]"}`} />
                          <p className="text-sm font-semibold">{streak.headline}</p>
                        </div>
                        <div className="mt-2 flex items-center gap-2">
                          <FormStrip form={streak.recent_form} />
                          <span className="text-[10px] font-semibold uppercase text-[#7c847f]">{streak.scope}</span>
                        </div>
                      </div>
                      <div>
                        <span className="inline-flex rounded bg-[#e1f2ea] px-2 py-1 text-[10px] font-bold uppercase text-[#19875c]">
                          {confidenceFor(streak)}
                        </span>
                      </div>
                      <div className="flex items-center justify-between md:block">
                        <div>
                          <p className="text-xs font-semibold">{streak.next_is_home ? "vs." : "at"} {streak.next_opponent || "TBD"}</p>
                          <p className="mt-1 text-[10px] text-[#66706a]">{formatKickoff(streak.next_match_date)}</p>
                        </div>
                        <ArrowRight className="h-4 w-4 text-[#175c43] md:mt-2" />
                      </div>
                    </article>
                  )
                })
              )}
            </div>

            <aside className="self-start rounded-md bg-[#10241b] p-5 text-white">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-[10px] font-bold text-[#c7f36b]">NEXT 72 HOURS</p>
                  <h2 className="mt-1 text-lg font-bold">Breaking soon</h2>
                </div>
                <Flame className="h-5 w-5 text-[#c7f36b]" />
              </div>
              <p className="mt-2 text-xs leading-5 text-white/55">High-signal streaks facing their next test.</p>

              <div className="mt-5 divide-y divide-white/10 border-y border-white/10">
                {breakingSoon.map((streak) => (
                  <div key={`breaking-${streak.streak_id}`} className="py-4">
                    <div className="flex items-start gap-3">
                      <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded bg-white p-1.5">
                        {streak.team_logo_url ? <img src={streak.team_logo_url} alt="" className="h-full w-full object-contain" /> : <Sparkles className="h-4 w-4 text-[#175c43]" />}
                      </span>
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-sm font-semibold">{streak.team_name}</p>
                        <p className="mt-0.5 truncate text-[11px] text-white/55">{streak.headline}</p>
                      </div>
                      <span className="rounded bg-[#c7f36b] px-1.5 py-1 text-[10px] font-bold text-[#10241b]">
                        {streak.current_streak_length}
                      </span>
                    </div>
                    <div className="mt-3 flex items-center justify-between text-[11px]">
                      <span className="text-white/65">{streak.next_is_home ? "vs." : "at"} {streak.next_opponent}</span>
                      <span className="font-semibold text-[#c7f36b]">{formatKickoff(streak.next_match_date).split(",")[0]}</span>
                    </div>
                  </div>
                ))}
              </div>

              <a href="/explore" className="mt-5 flex h-11 items-center justify-center gap-2 rounded-md border border-white/15 text-xs font-semibold hover:border-[#c7f36b]">
                View fixture calendar <ArrowRight className="h-4 w-4" />
              </a>
            </aside>
          </div>

          <div className="mt-5 grid gap-3 sm:grid-cols-3">
            {[
              [TrendingUp, "Signal strength", "Length, recency and consistency"],
              [ShieldCheck, "Context checked", "Home, away and opponent strength"],
              [CircleUserRound, "Team intelligence", "Every streak links to deeper analysis"],
            ].map(([Icon, title, copy]) => {
              const FeatureIcon = Icon as typeof TrendingUp
              return (
                <div key={title as string} className="flex items-start gap-3 border-t border-[#dce2dd] px-1 py-4">
                  <FeatureIcon className="mt-0.5 h-4 w-4 shrink-0 text-[#175c43]" />
                  <div><p className="text-xs font-semibold">{title as string}</p><p className="mt-1 text-[11px] text-[#66706a]">{copy as string}</p></div>
                </div>
              )
            })}
          </div>
        </section>
      </div>
    </main>
  )
}
