// Browser-only fixture interception. Never imported by the application or API.
// PLAYWRIGHT_MODULE may point to an existing installation; no package install required.
import assert from "node:assert/strict"
import { createRequire } from "node:module"
const require = createRequire(import.meta.url)
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright")
const base = process.env.FORECAST_PAGE_URL || "http://localhost:3102"
const browser = await chromium.launch({ headless: true, ...(process.env.PLAYWRIGHT_CHANNEL ? { channel: process.env.PLAYWRIGHT_CHANNEL } : {}) })
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
page.setDefaultTimeout(120000)
page.setDefaultNavigationTimeout(120000)
const asOf = "2026-10-04T10:00:00Z"
const statuses = { pending: 0, hit: 26, miss: 0, void: 0, unavailable: 0, excluded: 0 }
const forecast = { entry_id: 1, event_key: "match:over:1.5", event_label: "Over 1.5 goals",
  probability: .75, captured_at: "2026-10-01T10:00:00Z", captured_kickoff: "2026-10-02T18:00:00Z",
  status: "hit", reason: "regulation_time_score", outcome_observed_at: "2026-10-02T21:00:00Z",
  evidence: { fixture: { home_team_fulltime_goal: 2, away_team_fulltime_goal: 1 } }, result_observation_state: "observed" }
const fixtures = Array.from({ length: 26 }, (_, i) => ({ fixture_id: i+1,
  home: i ? `Test home ${i}` : "Brooklyn", away: "Detroit", country: "USA", league: "USL",
  kickoff: "2026-10-02T18:00:00Z", selection_cutoff: "2026-10-02T18:00:00Z", selection_state: "final",
  forecasts: [{ ...forecast, entry_id: i+1 }] }))
let mode = "populated", listCalls = []
await page.route("**/api/forecast-record**", async route => {
  const url = new URL(route.request().url())
  if (mode === "unavailable") return route.fulfill({ status: 503, json: { error: "The forecast record is not connected yet." } })
  if (url.pathname.endsWith("/filters")) return route.fulfill({ json: { as_of: asOf, leagues: [{ country: "USA", league: "USL" }] } })
  if (url.pathname.endsWith("/history")) return route.fulfill({ json: { as_of: asOf,
    forecast: { id: 1, probability: .75, captured_at: forecast.captured_at }, observations: [
      { id: 1, observed_at: forecast.outcome_observed_at, status: "hit", reason: "regulation_time_score", evidence: forecast.evidence },
      { id: 2, observed_at: "2026-10-03T10:00:00Z", status: "miss", reason: "regulation_time_score",
        evidence: { fixture: { home_team_fulltime_goal: 1, away_team_fulltime_goal: 0 } } },
    ], next_after_id: null } })
  listCalls.push(url)
  const filtered = url.searchParams.get("team") === "Nobody" ? [] : fixtures
  const offset = Number(url.searchParams.get("offset") || 0)
  return route.fulfill({ json: { as_of: asOf, policy: "latest_pre_kickoff_dc_goals_v1",
    totals: { fixtures: filtered.length, forecasts: filtered.length },
    cohort: { fixtures: filtered.length, forecasts: filtered.length, status_counts: { ...statuses, hit: filtered.length } },
    pagination: { offset, limit: 25, next_offset: filtered.length > offset+25 ? offset+25 : null }, fixtures: filtered.slice(offset,offset+25) } })
})
try {
  console.log("Browser ready; opening forecast page")
  await page.goto(`${base}/forecast-record`)
  console.log("Page loaded; checking record interactions")
  await page.getByRole("heading", { name: "26 fixtures · 26 forecasts" }).waitFor()
  await page.getByRole("button", { name: "1 forecast +", exact: true }).first().click()
  await page.getByRole("heading", { name: "Over 1.5 goals" }).waitFor()
  await page.getByRole("button", { name: "Outcome & correction history" }).click()
  await page.getByText("Miss · 1–0", { exact: true }).waitFor()
  await page.getByRole("button", { name: "Load 25 more fixtures" }).click()
  await page.getByText("Showing 26 of 26 fixtures", { exact: false }).waitFor()
  assert.equal(listCalls.at(-1).searchParams.get("as_of"),asOf)
  await page.getByRole("button", { name: "Settled", exact: true }).click()
  await page.waitForURL("**outcome=settled")
  await page.getByRole("heading", { name: "26 fixtures · 26 forecasts" }).waitFor()
  await page.getByText("All leagues ▾", { exact: true }).click()
  await page.getByText("USA", { exact: true }).click()
  await page.getByRole("button", { name: "USL", exact: true }).click()
  await page.waitForURL("**country=USA")
  await page.getByRole("searchbox", { name: "Search teams" }).fill("Nobody")
  await page.getByRole("button", { name: "Apply filters" }).click()
  await page.getByRole("heading", { name: "No captured forecasts match these filters" }).waitFor()
  assert.equal(listCalls.at(-1).searchParams.get("league"),"USL")
  await page.getByRole("button", { name: "Clear filters" }).click()
  await page.getByRole("heading", { name: "26 fixtures · 26 forecasts" }).waitFor()
  await page.getByRole("button", { name: "1 forecast +", exact: true }).first().click()
  await page.getByText("Recorded score 2–1.", { exact: false }).waitFor()
  await page.screenshot({ path: "/private/tmp/forecast-record-desktop.png", animations: "disabled" })
  await page.getByRole("button", { name: "Hide forecasts −", exact: true }).first().click()
  await page.setViewportSize({ width:390,height:844 })
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true)
  await page.getByRole("button", { name: "1 forecast +", exact: true }).first().click()
  await page.screenshot({ path: "/private/tmp/forecast-record-mobile.png" })
  mode = "unavailable"
  await page.getByRole("button", { name: "Refresh record" }).click()
  await page.getByRole("heading", { name: "Forecast record unavailable" }).waitFor()
  assert.equal(await page.getByRole("heading",{name:"26 fixtures · 26 forecasts"}).count(),0)
  console.log("PASS: expansion, corrections, pagination/as_of, combined filters, empty/error states, mobile overflow")
} catch (error) {
  try {
    console.error("Browser page title:", await page.title())
    console.error((await page.locator("body").innerText({ timeout:5000 })).slice(0,1500))
    await page.screenshot({ path: "/private/tmp/forecast-record-failure.png", timeout:5000 })
  } catch { console.error("Page was not available for failure diagnostics") }
  throw error
} finally { await browser.close() }
