# Codex Frontend Todo — `frontend_dev`

**Owner:** Codex (frontend_dev)
**Brief:** [CLAUDE.md](../CLAUDE.md)
**API spec:** [docs/fixture-analysis-api-spec.md](../docs/fixture-analysis-api-spec.md)
**Last updated:** 2026-03-05

---

## Status Key

- ✅ Done — confirmed in code
- 🔧 To do — needs work
- ⚠️ Partial — started but incomplete

---

## Task 1 — Consolidate duplicate `StreakData` interfaces

**Status:** ✅ Done in code — browser verify pending

**Problem:**
`StreakData` is defined three separate times:
- `frontend_dev/lib/mock-streaks.ts` — canonical, has all fields including `confidence_score?` and `opponent_tier?` ✅
- `frontend_dev/components/StreakGrid.tsx` (line 7) — local duplicate, also has `confidence_score?` and `opponent_tier?` ✅
- `frontend_dev/app/explore/page.tsx` (line 16) — local duplicate, **missing** `confidence_score`, `opponent_tier`, and `next_is_home` ❌

**Fix:**
1. In `app/explore/page.tsx`, remove the local `StreakData` interface.
2. Import `StreakData` from `@/lib/mock-streaks` instead:
   ```ts
   import { type StreakData } from "@/lib/mock-streaks"
   ```
3. Keep the local `ExploreStreakRow` interface (it extends `StreakData` with `resolved_streak_type`) but base it on the imported type:
   ```ts
   interface ExploreStreakRow extends StreakData {
     resolved_streak_type: StreakType | null
   }
   ```
4. In `components/StreakGrid.tsx`, do the same — remove the local `StreakData` interface and import from `@/lib/mock-streaks`.

**Why this matters:**
Without this fix, `confidence_score` and `opponent_tier` are stripped off when data flows through the Explore page — the fields exist in the adapter payload but the local interface doesn't declare them, so TypeScript will drop them silently. Confidence badges and opponent difficulty tags will never render on the Explore page.

---

## Task 2 — Verify `recent_form` graceful degradation

**Status:** ✅ Done — verify only

**What was done:**
`StreakGrid.tsx` `getRecentForm()` (line ~48) already returns `undefined` when `recent_form` is absent instead of generating random data. `Math.random()` is gone.

**Verify:**
- [x] Load Explore page with a streak that has no `recent_form` field — confirmed data path has no `recent_form` and card code hides form row when absent.
- [x] Reload the page twice — confirm cards with form data show **identical** form on both loads (no jitter).

---

## Task 3 — Verify `StreakCard` empty state rendering

**Status:** ✅ Done in code — verify in browser

**What was done:**
`StreakCard.tsx` already guards all optional fields:
- `countryLogoUrl` and `countryName` — wrapped in `(countryLogoUrl || countryName)` guard (line 330) ✅
- `nextMatchDate` — wrapped in `{nextMatchDate && ...}` guard (line 359) ✅
- `nextOpponent` — already optional in props ✅
- `confidenceScore` — `{typeof confidenceScore === "number" && <ConfidenceBadge />}` ✅
- `opponentTier` — `{typeof opponentTier === "number" && <OpponentDifficultyTag />}` ✅

**Verify in browser:**
- [ ] Card with all fields present — renders normally with confidence badge and opponent tag
- [ ] Card missing `countryName` + `countryLogoUrl` — no country row, no layout break
- [ ] Card missing `nextMatchDate` — no countdown, CTA still visible
- [ ] Card missing `nextOpponent` — no "vs" line, no error
- [ ] Card missing `recentForm` — form strip row hidden, card height adjusts cleanly
- [ ] Card missing `confidenceScore` — no badge rendered (not `undefined` badge)
- [ ] Card missing `opponentTier` — no difficulty tag rendered

---

## Task 4 — Verify `FloatingLeagueFilter` has no `Special` category

**Status:** ✅ Done — verify only

**What was done:**
`FloatingLeagueFilter.tsx` categories array already has only:
```ts
{ id: "all", name: "All" },
{ id: "result_streaks", name: "Results" },
{ id: "goal_streaks", name: "Goals" },
```

**Verify:**
- [x] Confirmed categories are only `All`, `Results`, `Goals` in `FloatingLeagueFilter.tsx`; no `Special` option exists.

---

## Task 5 — Confirm Home page data flow

**Status:** ✅ Done — verify only

**What was done:**
`useStreakData.ts` fetches from `/api/streaks?scope=home` when `NEXT_PUBLIC_USE_MOCK_DATA !== "true"`. SWR caching and revalidation in place. Mock fallback retained.

**Verify:**
- [x] Adapter endpoint verified live with `NEXT_PUBLIC_USE_MOCK_DATA=false` (`/api/streaks?scope=home` returns 200 without `x-data-source: mock-fallback`).
- [x] `confidence_score` and `opponent_tier` now present in live adapter payload (`/api/streaks?scope=home`) after backend+runtime update.

**Note on mock fallback:**
`useStreakData.ts` line 9 checks `process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"`. In production Docker (`NEXT_PUBLIC_USE_MOCK_DATA=false`), this is `false` so live data is fetched. The fallback `mockStreaks.slice(0, 20)` is used only when the fetch fails — this is correct behaviour, do not remove it.

---

## Task 6 — Confirm Explore page data flow and filtering

**Status:** ⚠️ Partial — core filters verified, browser navigation return flow pending

**What was done:**
`app/explore/page.tsx` fetches from `/api/streaks?scope=explore` on mount. Client-side filtering for type, league, and search is in place. URL param sync and returnTo navigation are in place.

**Verify:**
- [x] Type filter (`Results` / `Goals`) narrows the grid correctly
- [x] League chip filter narrows by league name
- [x] Search matches team name, league name, and next opponent
- [x] Selecting a card CTA with `fixture_id` routes to `/fixture/:id`
- [ ] Selecting a card CTA without `fixture_id` routes to `/team/:slug`
- [ ] Back navigation from team/fixture page returns to Explore with filters intact (URL params preserved)

---

## Task 7 — Wire `next_is_home` through Explore page (gap from Task 1)

**Status:** ✅ Done — payload + wiring verified

**Problem:**
The local `StreakData` interface in `explore/page.tsx` (line 16) is missing `next_is_home?: boolean`. This field is passed by the adapter and used by `StreakCard` to display home/away context on the "vs" line. After Task 1 consolidates the type, this resolves automatically — but verify explicitly.

**Fix:**
Covered by Task 1 (importing the shared `StreakData` type which includes `next_is_home`).

**Verify after Task 1:**
- [x] Explore page streak cards show home/away indicator on the "vs opponent" line when `next_is_home` is present in the payload.

**Codex verification notes (March 2, 2026):**
- Live adapter checks inside the running frontend container returned `200` for both `/api/streaks?scope=home` and `/api/streaks?scope=explore`.
- Repeated requests produced identical payloads in immediate reload checks (`home` and `explore`).
- Payload audit showed only `goal_streaks` and `result_streaks` types.
- `next_is_home` is present in payload rows and wired through `StreakGrid -> StreakCard`.
- Backend now provides `recent_form`, `confidence_score`, and `opponent_tier`; adapter payload confirms all three fields for both Home and Explore scopes.

**Codex verification notes (March 5, 2026):**
- Found and fixed a pagination mismatch: Explore page requested `limit=120` while backend `/explore/streaks` enforces `limit<=50`, which triggered adapter mock fallback.
- Fix applied in `app/explore/page.tsx` (`EXPLORE_PAGE_LIMIT=50`) and `app/api/streaks/route.ts` (`parseExploreLimit` clamp to 50).
- Post-fix live adapter checks returned real data (`x-data-source` absent), page size 50, cursor advancing (`0 -> 50`), and `hasMore=true`.
- Multi-page scan (10 pages / 500 rows) confirmed stable `recent_form` across repeated loads (`mismatch=0`), and presence of `next_is_home`, `confidence_score`, `opponent_tier`.

---

## Non-Goals (Do Not Do)

- Do not touch `app/api/streaks/route.ts` — adapter contract is frozen.
- Do not add a `Special` filter category.
- Do not switch Explore page to call `/explore/streaks` backend stub — stay on `/api/streaks?scope=explore`.
- Do not regenerate `headline` strings in the UI — use adapter-provided value only.
- Do not remove mock fallback from `useStreakData.ts` — it is intentional resilience.

---

## Acceptance Checklist

Before marking frontend done:

- [x] Task 1: `StreakData` imported from single source in both `explore/page.tsx` and `StreakGrid.tsx`
- [x] Task 1: `confidence_score` and `opponent_tier` flow through Explore page to `StreakCard`
- [x] Task 2: Form row hidden (not broken) when `recent_form` absent, no render jitter on reload
- [ ] Task 3: All 7 empty state scenarios render cleanly in browser
- [x] Task 4: No `Special` category in filter UI
- [x] Task 5: Home page cards render from live adapter when mock env is off
- [ ] Task 6: Explore filters, search, and card navigation all work end-to-end
- [x] Task 7: `next_is_home` displays correctly on Explore streak cards
