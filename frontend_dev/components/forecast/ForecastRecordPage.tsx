"use client"

import Link from "next/link"
import { ProductHeader } from "@/components/account/ProductHeader"
import { SignedOut } from "@clerk/nextjs"
import { usePathname, useRouter, useSearchParams } from "next/navigation"
import { Fragment, useEffect, useRef, useState } from "react"
import type { ComponentProps, FormEvent } from "react"
import { GlassButton } from "@/components/GlassButton"
import { directorySchema, historySchema, recordSchema, reasonLabel, score, statusLabel } from "@/lib/forecast-record"
import type { Fixture, Forecast, ForecastRecord, History } from "@/lib/forecast-record"
import styles from "./forecast-record.module.css"
import { FunctionalityTip } from "@/components/research/FunctionalityTip"
import { Bookmark, CalendarDays, ChartNoAxesCombined, Compass, PanelLeftClose, PanelLeftOpen } from "lucide-react"

const navigation = [
  { name: "Discover", href: "/discover", Icon: Compass, mobileIcon: "◎" },
  { name: "Matches", href: "/matches", Icon: CalendarDays, mobileIcon: "☰" },
  { name: "Forecast record", href: "/forecast-record", Icon: ChartNoAxesCombined, mobileIcon: "◷" },
  { name: "Saved", href: "/saved", Icon: Bookmark, mobileIcon: "☆" },
]

function Button(props: ComponentProps<typeof GlassButton>) {
  return <GlassButton variant="outline" {...props} className={`${styles.button} ${props.className || ""}`} />
}
function stamp(value: string, zone: string, dateOnly = false) {
  return new Intl.DateTimeFormat("en", { day: "numeric", month: "short", year: "numeric", timeZone: zone,
    ...(dateOnly ? {} : { hour: "2-digit", minute: "2-digit", timeZoneName: "short" }) }).format(new Date(value))
}
async function readJson(url: string, signal?: AbortSignal) {
  const response = await fetch(url, { cache: "no-store", signal })
  const body = await response.json().catch(() => null)
  if (!response.ok) throw new Error(typeof body?.error === "string" ? body.error : "The forecast record is unavailable.")
  return body
}

function CorrectionHistory({ forecast, asOf, zone }: { forecast: Forecast; asOf: string; zone: string }) {
  const [open, setOpen] = useState(false)
  const [data, setData] = useState<History | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState("")
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => controller.current?.abort(), [])
  async function load(after?: number) {
    controller.current?.abort()
    const abort = new AbortController(); controller.current = abort
    setBusy(true); setError("")
    const params = new URLSearchParams({ as_of: asOf, limit: "25" })
    if (after) params.set("after_id", String(after))
    try {
      const next = historySchema.parse(await readJson(`/api/forecast-record/entries/${forecast.entry_id}/history?${params}`, abort.signal))
      setData(old => after && old ? { ...next, observations: [...old.observations, ...next.observations] } : next)
    } catch (e) { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "History unavailable.") }
    finally { if (!abort.signal.aborted) setBusy(false) }
  }
  return <div className={styles.history}>
    <Button aria-expanded={open} aria-controls={`history-${forecast.entry_id}`} onClick={() => {
      setOpen(!open); if (!open && !data) void load()
    }}>{open ? "Hide history" : "Outcome & correction history"}</Button>
    {open && <div id={`history-${forecast.entry_id}`}>
      <p className={styles.caption}>Every recorded outcome, including corrections. The captured estimate stays unchanged.</p>
      {data?.observations.map(row => <div key={row.id} className={styles.observation}>
        <strong>{statusLabel[row.status]}{score(row.evidence) ? ` · ${score(row.evidence)}` : ""}</strong>
        <p>{reasonLabel(row.reason)}</p><time dateTime={row.observed_at}>{stamp(row.observed_at, zone)}</time>
      </div>)}
      {data?.observations.length === 0 && <p>No outcome observations have been recorded yet.</p>}
      {error && <p role="alert">{error}</p>}
      {busy && <p role="status">Loading history…</p>}
      {!busy && (error || data?.next_after_id != null) && <Button onClick={() => void load(data?.next_after_id ?? undefined)}>
        {error ? "Retry history" : "Load more observations"}</Button>}
    </div>}
  </div>
}

function FixtureRow({ fixture, asOf, zone }: { fixture: Fixture; asOf: string; zone: string }) {
  const [open, setOpen] = useState(false)
  const settled = fixture.forecasts.filter(f => f.status === "hit" || f.status === "miss").length
  const pending = fixture.forecasts.filter(f => f.status === "pending").length
  const other = fixture.forecasts.length - settled - pending
  const summary = [settled && `${settled} settled`, pending && `${pending} pending`, other && `${other} other`].filter(Boolean).join(" · ")
  return <article className={styles.fixture}>
    <div className={styles.fixtureSummary}>
      <div className={styles.identity}><h2>{fixture.home || "Home team unavailable"} vs {fixture.away || "Away team unavailable"}</h2>
        <p className={styles.caption}>{[fixture.country, fixture.league].filter(Boolean).join(" · ")} · {stamp(fixture.kickoff, zone)}</p></div>
      <p className={styles.summary}>{summary}</p>
      <Button aria-expanded={open} aria-controls={`fixture-${fixture.fixture_id}`} onClick={() => setOpen(!open)}>
        {open ? "Hide forecasts −" : `${fixture.forecasts.length} forecast${fixture.forecasts.length === 1 ? "" : "s"} +`}</Button>
    </div>
    {open && <div id={`fixture-${fixture.fixture_id}`} className={styles.events}>
      <p className={styles.eventHeading}>{fixture.selection_state === "provisional" ? "LATEST CAPTURED FORECASTS" : "LAST FORECASTS BEFORE KICKOFF"} · OUTCOMES</p>
      {fixture.selection_state === "provisional" && <p className={styles.caption}>Selection is still provisional and may change before the capture cutoff.</p>}
      {fixture.forecasts.map(f => <section className={styles.event} key={f.entry_id}>
        <div className={styles.eventTitle}><h3>{f.event_label || f.event_key}</h3><strong>{(f.probability * 100).toFixed(1)}%</strong>
          <span className={styles.badge} data-status={f.status}>{statusLabel[f.status]}</span></div>
        <p className={styles.caption}>Captured <time dateTime={f.captured_at}>{stamp(f.captured_at, zone)}</time></p>
        <p>{score(f.evidence) ? `Recorded score ${score(f.evidence)}. ` : ""}{reasonLabel(f.reason)}</p>
        <p className={styles.caption}>{f.outcome_observed_at
          ? `Last changed outcome: ${stamp(f.outcome_observed_at, zone)}. This is not the last check time.`
          : "No result observation recorded yet."}</p>
        <CorrectionHistory forecast={f} asOf={asOf} zone={zone} />
      </section>)}
    </div>}
  </article>
}

/** Responsive implementation of the approved record flow; never substitutes mock records. */
export function ForecastRecordPage() {
  const [collapsed, setCollapsed] = useState(false)
  useEffect(() => {
    try { setCollapsed(localStorage.getItem("ballistics:sidebar:collapsed") === "true") } catch { /* Use expanded default. */ }
  }, [])
  const toggleSidebar = () => setCollapsed(current => {
    const next = !current
    try { localStorage.setItem("ballistics:sidebar:collapsed", String(next)) } catch { /* Navigation still works. */ }
    return next
  })
  const search = useSearchParams(), router = useRouter(), pathname = usePathname()
  const query = search.toString()
  const [data, setData] = useState<ForecastRecord | null>(null)
  const [dataKey, setDataKey] = useState("")
  const [busy, setBusy] = useState(true), [error, setError] = useState("")
  const [refresh, setRefresh] = useState(0)
  const [zone, setZone] = useState("UTC")
  const [leagues, setLeagues] = useState<{ country: string | null; league: string }[]>([])
  const [directoryError, setDirectoryError] = useState("")
  const [directoryBusy, setDirectoryBusy] = useState(true)
  const [leagueSearch, setLeagueSearch] = useState("")
  const controller = useRef<AbortController | null>(null)
  const visible = dataKey === query ? data : null
  const outcome = search.get("outcome") || "all"
  useEffect(() => { setZone(Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC") }, [])
  useEffect(() => {
    const abort = new AbortController(); controller.current = abort
    setData(null); setBusy(true); setError("")
    const params = new URLSearchParams(query); params.delete("as_of"); params.set("offset", "0"); params.set("limit", "25")
    readJson(`/api/forecast-record?${params}`, abort.signal).then(body => {
      if (abort.signal.aborted) return
      setData(recordSchema.parse(body)); setDataKey(query)
    }).catch(e => { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "Unable to load forecasts.") })
      .finally(() => { if (!abort.signal.aborted) setBusy(false) })
    return () => abort.abort()
  }, [query, refresh])
  useEffect(() => {
    const abort = new AbortController()
    setDirectoryError(""); setDirectoryBusy(true)
    readJson("/api/forecast-record/filters", abort.signal).then(body => { if (!abort.signal.aborted) setLeagues(directorySchema.parse(body).leagues) })
      .catch(() => { if (!abort.signal.aborted) setDirectoryError("League directory unavailable. Retry with Refresh record.") })
      .finally(() => { if (!abort.signal.aborted) setDirectoryBusy(false) })
    return () => abort.abort()
  }, [refresh])
  function navigate(params: URLSearchParams) {
    params.delete("offset"); params.delete("as_of"); params.delete("limit")
    router.replace(`${pathname}${params.size ? `?${params}` : ""}`, { scroll: false })
  }
  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const form = new FormData(event.currentTarget), params = new URLSearchParams(query)
    for (const field of ["date_from", "date_to", "team"]) {
      const value = String(form.get(field) || "").trim()
      if (value) params.set(field, value); else params.delete(field)
    }
    navigate(params)
  }
  async function more() {
    if (!visible || visible.pagination.next_offset == null || busy) return
    const abort = controller.current
    if (!abort) return
    setBusy(true); setError("")
    const params = new URLSearchParams(query)
    params.set("as_of", visible.as_of); params.set("offset", String(visible.pagination.next_offset)); params.set("limit", "25")
    try {
      const next = recordSchema.parse(await readJson(`/api/forecast-record?${params}`, abort.signal))
      if (!abort.signal.aborted) setData(old => old ? { ...next, fixtures: [...old.fixtures, ...next.fixtures] } : next)
    } catch (e) { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "Unable to load more.") }
    finally { if (!abort.signal.aborted) setBusy(false) }
  }
  const countries = Array.from(new Set(leagues.filter(l => `${l.country} ${l.league}`.toLowerCase().includes(leagueSearch.toLowerCase())).map(l => l.country))).sort()
  const counts = visible?.cohort.status_counts
  return <div className={styles.page}>
    <a className={styles.skip} href="#forecast-content">Skip to forecast record</a>
    <ProductHeader/>
    <div className={`${styles.workspace} ${collapsed ? styles.workspaceCollapsed : ""}`}>
      <nav className={styles.sidebar} aria-label="Primary">
        <button type="button" className={styles.sidebarToggle} onClick={toggleSidebar} aria-label={collapsed ? "Expand navigation" : "Collapse navigation"} aria-expanded={!collapsed} title={collapsed ? "Expand navigation" : "Collapse navigation"}>
          {collapsed ? <PanelLeftOpen size={20} aria-hidden="true"/> : <PanelLeftClose size={20} aria-hidden="true"/>}
          <span className={styles.srOnly}>{collapsed ? "Expand menu" : "Collapse menu"}</span>
        </button>
        {navigation.map(({ name, href, Icon }) => <Button asChild key={name} className={`${styles.sidebarLink} ${name === "Forecast record" ? styles.active : ""}`}>
          <Link href={href} aria-label={collapsed ? name : undefined} aria-current={name === "Forecast record" ? "page" : undefined} title={collapsed ? name : undefined}>
            <Icon size={20} aria-hidden="true"/><span className={collapsed ? styles.srOnly : ""}>{name}</span>
          </Link>
        </Button>)}
      </nav>
      <main id="forecast-content" className={styles.main}>
        <h1>Forecast record</h1><p className={styles.subtitle}>What we recorded. What happened next.</p>
        <a href="#record-methodology">How it works →</a>
        <div className={styles.notice}>Captured forecasts, including misses. Estimates are not guarantees. No historical results are reconstructed.</div>
        <form key={query} onSubmit={apply} className={styles.filters} aria-label="Filter forecasts">
          <label>From (UTC)<input type="date" name="date_from" defaultValue={search.get("date_from") || ""} /></label>
          <label>To (UTC)<input type="date" name="date_to" defaultValue={search.get("date_to") || ""} /></label>
          <label>Search teams<input type="search" name="team" maxLength={100} defaultValue={search.get("team") || ""} placeholder="Either team" /></label>
          <Button type="submit">Apply filters</Button>
        </form>
        <div className={styles.tools}>
          <details className={styles.leaguePicker}><summary>{search.get("league") || "All leagues"} ▾</summary>
            <div className={styles.leaguePanel}><label>Find a country or league<input type="search" value={leagueSearch} onChange={e => setLeagueSearch(e.target.value)} /></label>
              <Button onClick={() => { const p = new URLSearchParams(query); p.delete("league"); p.delete("country"); navigate(p) }}>All leagues</Button>
              {directoryError && <p role="status">{directoryError}</p>}
              {directoryBusy && <p role="status">Loading captured leagues…</p>}
              {!directoryBusy && !directoryError && !leagues.length && <p>No captured leagues available.</p>}
              {countries.map(country => <details key={country || "unknown"}><summary>{country || "Country unavailable"}</summary>
                {leagues.filter(l => l.country === country && `${l.country} ${l.league}`.toLowerCase().includes(leagueSearch.toLowerCase())).map(l =>
                  <Button key={l.league} onClick={e => {
                    const p = new URLSearchParams(query); p.set("league", l.league)
                    if (l.country) p.set("country", l.country); else p.delete("country")
                    navigate(p); e.currentTarget.closest(`.${styles.leaguePicker}`)?.removeAttribute("open")
                  }}>{l.league}</Button>)}
              </details>)}
            </div>
          </details>
          <Button onClick={() => navigate(new URLSearchParams())}>Clear filters</Button>
          <Button disabled={busy} onClick={() => setRefresh(r => r + 1)}>Refresh record</Button>
        </div>
        <p className={styles.caption}>Date filters use archived kickoff dates in UTC. Times displayed in {zone}.</p>
        <div className={styles.tabs} aria-label="Outcome filters">{["all", "settled", "pending", "other"].map(tab =>
          <Button key={tab} aria-pressed={outcome === tab} className={outcome === tab ? styles.active : ""} onClick={() => {
            const p = new URLSearchParams(query); p.set("outcome", tab); navigate(p)
          }}>{tab[0].toUpperCase() + tab.slice(1)}</Button>)}</div>
        {error && <div className={styles.notice} role="alert"><h2>Forecast record unavailable</h2><p>{error}</p>
          <SignedOut><Link href="/sign-in">Sign in to continue →</Link></SignedOut>
          <Button disabled={busy} onClick={() => visible ? void more() : setRefresh(r => r + 1)}>Try again</Button>
        </div>}
        {busy && !visible && <div className={styles.loading} role="status">Loading captured forecasts…</div>}
        {visible && <>
          <FunctionalityTip id="record" title="Check the published record" moreHref="#record-methodology" moreLabel="Read the methodology"><p>This record includes captured forecasts, including misses. Settled entries have an outcome; pending entries do not yet. It shows how recorded forecasts turned out, not your personal betting history or a guarantee.</p></FunctionalityTip>
          <h2 aria-live="polite">{visible.totals.fixtures.toLocaleString()} fixtures · {visible.totals.forecasts.toLocaleString()} forecasts</h2>
          {counts && <p className={styles.caption}>{counts.hit + counts.miss} settled · {counts.pending} pending · {counts.void + counts.unavailable + counts.excluded} other forecasts in this date/team/league selection</p>}
          <p className={styles.caption}>Showing {visible.fixtures.length} of {visible.totals.fixtures} fixtures · Newest archived kickoff first</p>
          <p className={styles.caption}>Browsing snapshot: {stamp(visible.as_of, zone)}. Refresh to see new records.</p>
          {!visible.fixtures.length && <div className={styles.fixture}><h2>No captured forecasts match these filters</h2><p>Try another date or clear the filters. An empty record is not evidence that no matches took place.</p></div>}
          {visible.fixtures.map((fixture, i) => <Fragment key={`${visible.as_of}-${fixture.fixture_id}`}>
            {(!i || stamp(visible.fixtures[i - 1].kickoff, zone, true) !== stamp(fixture.kickoff, zone, true)) && <h2 className={styles.day}>{stamp(fixture.kickoff, zone, true)}</h2>}
            <FixtureRow fixture={fixture} asOf={visible.as_of} zone={zone} />
          </Fragment>)}
          {visible.pagination.next_offset != null && <Button disabled={busy} onClick={() => void more()}>{busy ? "Loading…" : "Load 25 more fixtures"}</Button>}
          <p className={styles.caption}>Counts cover all matching forecasts, including those not yet loaded. Related goal events are not independent. No combined accuracy claim is made.</p>
        </>}
        <details id="record-methodology" className={styles.methodology}><summary>How this record works</summary>
          <p>We evaluate the latest eligible forecast captured before kickoff for each match and event. Earlier versions remain stored. Before the capture cutoff, selection is provisional.</p>
          <p>Hit and Miss describe the event outcome—not whether a probability was “correct”. Pending, Void, Unavailable and Excluded are not counted as settled binary outcomes.</p>
          <p>Result corrections are appended to the history without rewriting the captured estimate. The last changed outcome time is not the last result-check time. Schedule changes use a conservative cutoff: a passed captured kickoff does not reopen.</p>
          <p>This record includes only goal forecasts saved for review, so it may not cover every estimate calculated for a match. A saved forecast was not necessarily shown to a user.</p>
        </details>
      </main>
    </div>
    <nav className={styles.bottomNav} aria-label="Mobile primary">{navigation.map(({ name, href, mobileIcon }) => <Link key={name} href={href} aria-current={name === "Forecast record" ? "page" : undefined}><span aria-hidden="true">{mobileIcon}</span>{name === "Forecast record" ? "Record" : name}</Link>)}</nav>
  </div>
}
