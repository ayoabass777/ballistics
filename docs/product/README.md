# Ballistics product memory

2026-10-22 — Ballistics 2.0 frontend source is moving into this repository at [`frontend_dev`](../../frontend_dev). The account API remains in the sibling `football_stats_project` repository. See [implementation status](implementation-status.md) for verification and deployment state.

2026-10-21 — D147 code-first invite-only beta entry is implemented locally. A real invitation was accepted by Clerk; the invited account signed in and reached Discover. The local profile-sync trigger has been added and awaits live backfill verification; Google is still offered on sign-in. See [decision log](decisions.md) and [implementation status](implementation-status.md).

2026-10-20 — Owner chose an invite-only beta with free Clerk account access and no card, billing or seven-day trial clock. The proposed seven-day trial and $9.99/$19.99 monthly price will be revisited at the end of product development. [Ship checkpoints](ship-checkpoints.md) now keep billing off the beta critical path; [decision log](decisions.md) records the agreed and pending parts separately.

2026-10-20 — Saved return-state verified and frontend/API committed as `891a2f2` in `football_stats_project`: session-local memory is keyed by account and filter, stores only page count/scroll metadata, and refetches those pages on return. Isolated synthetic browser QA passed 12→24→27 cards, end-of-list behavior, and mobile Match→Back restoring Upcoming, 24 cards and scroll Y≈7749. No horizontal document overflow at 390px or 320px. TypeScript, account proxy/research contract checks and 16 focused Python tests passed; rolled-back PostgreSQL pagination test passed. Synthetic QA is separate from real account data.

2026-10-20 — Launch planning now has [ship checkpoints and ETS review rules](ship-checkpoints.md). Initial ranges are 10–24 November 2026 for a possible invite-only member beta and 15 December 2026–12 January 2027 for a paid public launch, assuming one focused developer. These are planning estimates, not deployment authorization; scope, pricing, payment and deployment gates remain open.

2026-10-20 — Saved filters and pagination implemented locally: All, Upcoming (NS/TBD), Live, Finished (FT/AET/PEN), and Other statuses (including unknown). `/account/saved/page` uses descending `(saved_at,fixture_id)` keyset cursors, default 12/max 50, with filters applied in SQL before limiting and payload resolution. Latest and saved-baseline versions use separate LIMIT 1 lookups rather than fetching all history. `/account/saved/ids` provides lightweight membership for Save buttons beyond page one. Legacy list/mutation responses now return only the first 12 cards; the updated frontend uses the paged endpoint for Saved and IDs for membership. Requests are aborted when switching filters; load-more errors preserve loaded cards and offer retry. Verified: frontend TypeScript, proxy/contracts, 16 account/identity tests, rolled-back PostgreSQL 27-save pagination/filter/isolation/timestamp-tie test; live signed-in browser All and Finished states verified. Figma unchanged.

2026-10-20 — Saved now reads current fixture status independently from its immutable saved card. Meaningful card versions are published atomically with match analysis into `raw.card_versions`, scoped by fixture/list/team/event/context/market. Run dates, logos, names and private ranking scores do not change the content hash; kickoff and public pick evidence do. Identical consecutive content reuses the version; A→B→A produces three events. Saved links matching content observed before save time and shows “Updated since you saved” for a later comparable version, retaining the original snapshot. Legacy unmatched cards remain explicitly unnumbered. Local verification: 16 account/identity tests, frontend typecheck and account proxy checks, real existing saves read successfully, PostgreSQL version-transition test passed and rolled back. API restarted locally. Browser visual verification blocked by locked Mac; Figma unchanged. See [card-version runbook](../saved-card-versions.md).

2026-10-19 — Dependency-aware engine reuse is implemented and live cold/warm/forced comparisons pass for all three active models. Inputs include shared league statistics and actual DC decay ages; unchanged outputs retain their computation version. Existing daily runner is wired to use it, with a force-recompute escape hatch. No feed was regenerated during verification. [Details and limits](../model_processing_reuse.md).

2026-10-20 — Accounts are Clerk (D143). The in-house auth branch `feat/user-accounts-db` is closed; its Stripe mirror and paywall view now live in the account API (`football_stats_project/API/sql/billing.sql`), keyed on Clerk user IDs. Stripe checkout and webhook are next.

2026-10-19 — Regular Discover category cards now use team identity → finding → league/kickoff → actions in the local frontend and active 1440px desktop Figma screens. The lime ranked standout cards retain their stronger treatment. The running local Discover API was verified serving the latest complete 2026-10-18 engine run (generated 2026-10-19 01:08 UTC); the dated 10 Oct Figma fixture sample remains static and linked to its historical detail examples. See [implementation status](implementation-status.md).

2026-10-19 — Engine: D137 Big dog gap now counts home advantage (+65 Elo); D138 adds slight-favourite levels and league tiers as labels only (API done, frontend pending); D136 Dixon–Coles cross-check tracked in shadow in the daily email. Decision numbers were de-duplicated: product decisions moved to D139–D142, D143 reserved for user accounts, next free D144. See the top of [decisions](decisions.md) and [implementation status](implementation-status.md).

2026-10-19 — Matches now has local team search, timezone day groups, an applied-scope summary and an inclusive custom date range in the code. The active desktop Figma Matches screen shows the revised hierarchy and a static two-date review overlay; it is illustrative, not a live date input. See [implementation status](implementation-status.md), [data contract](data-contract.md) and [prototype links](prototype-links.md).

2026-10-19 — Full forecast archive executed and verified in private S3: 18 snapshots, 57,718 entries and 43,197 settlements. Batch checksums/read-back and manifests verified before per-table checkpoints advanced; archived counts reconciled to unchanged PostgreSQL source counts. Real Airflow incremental task passed with zero new rows, confirming checkpoint resume. `forecast_history_archive` is unpaused locally and scheduled daily at 05:45 UTC. Disposable PostgreSQL tests cover failed verification/retry and appended settlement correction. No source deletion, retention expiry or API archive fallback enabled. See [archive runbook](../forecast_archive.md).

2026-10-19 — Shared Saved snapshots are implemented locally and three existing saves migrated without changing their content/timestamps. Monthly partitions and guarded archive tooling are in place; forecast migration and retention activation remain pending. See [snapshot implementation and remaining scope](saved-forecast-retention-design.md).

2026-10-19 — D135 collapsible desktop sidebar is applied across desktop Figma screens, including Matches, details, Saved, Account and Forecast record. The owner identified that Matches `16:355` still had the older browse design; its active Figma content has now been replaced with an editable layout matching the implemented `/matches` page hierarchy. The old main content remains hidden as an archive. Fixture data in Figma is illustrative, and the full live filtering behavior remains in code. See the [sidebar handoff](desktop-sidebar-collapse-2026-10-18.md) for scope and limits.

2026-10-19 — Agreed design direction: shared immutable Saved/forecast snapshots referenced by stable fixture/event forecast IDs, date partitioning, and Postgres/S3 archival. See [retention and snapshot design](saved-forecast-retention-design.md). Numerical retention windows remain proposed; implementation is pending.

2026-10-18 — The owner approved the D135 compact desktop sidebar look on Discover. Collapse is implemented locally in the shared frontend shell and prototyped on the three desktop Discover Figma roots. Resume from the [sidebar handoff](desktop-sidebar-collapse-2026-10-18.md); [implementation status](implementation-status.md) separates verified work from remaining coverage.

2026-10-18 — Account-backed Saved and followed leagues are implemented locally in the original API/frontend, and the account database tables were verified. See the top entries in [implementation status](implementation-status.md), [data contract](data-contract.md) and D134 in [decisions](decisions.md). Authenticated browser verification and deployment remain open; email-only auth depends on the Clerk instance configuration. Older browser-local Saved statements below are historical.

2026-10-15 — Owner authorised frontend logo implementation. Discover, All standouts and Matches now use paired 24px club crests and 20px league logos with names; Match details uses the same sticky identity (stacked on mobile) plus 36px mobile title crests. Saved retains logo URLs for new saves; older saved records remain compatible. Discover reads fixture/league asset references from database joins; Match context adds league_logo_url. Public URLs use the existing crest store; no temporary Figma URLs or sample club substitutions. Missing/broken club images fall back to initials; missing/broken league logos leave text. Fixed screen-reader-only text escaping the carousel and causing page overflow. Live authenticated browser verified loaded asset geometry and Discover at 1440/390/320px without document overflow, plus mobile Match details. Scoped TypeScript, research contract/proxy tests and 35 backend tests passed. Evidence: mock/frontend-crests-2026-10-15.jpg. Local implementation only, not deployed.

2026-10-15 — Matches navigation now opens `/matches`, a chronological list of upcoming fixtures present in the current engine snapshot with shared date/league filters, detail links and Save actions. Discover categories, All standouts and Matches reveal 12 cards per batch. Session-local expansion and vertical scroll state are keyed by route, category and filters and restored after async data rendering. Browser verified Matches loading 504 fixtures, Strength 12→24 and match/back scroll restoration, Goals 12→24 and retention after switching to Patterns (independent 12). Six backend Discover tests, research contracts/proxy checks and scoped TypeScript passed. This is snapshot coverage, not a complete raw fixture browser; no deployment claimed.

2026-10-15 live integration: refreshed the real 812-fixture snapshot and connected the local frontend to the read-only API. Discover and 14 representative Match payloads validated. Signed-in Discover → Match details → Save → Saved → reopen → remove verified in the real browser on 2026-10-15. [Evidence and restart instructions](live-integration-2026-10-15.md).

2026-10-15 current handoff: the owner authorised the first frontend build. Discover → Match details → Saved is now locally implemented; see [build evidence and remaining integration work](frontend-build-2026-10-15.md). Saved is explicitly browser-local, live authenticated integration and deployment remain pending. Earlier design-only statements below are historical. Standouts retain the approved lime design and existing engine ranking.

Established: 2026-09-29. Owner: project owner. Scope: Ballistics 2.0 design and implementation.

Start here before continuing design or frontend work:

1. [Design brief](design-brief.md): product intent and experience principles.
2. [Decisions](decisions.md): agreed direction, proposals and superseded choices.
3. [Data contract](data-contract.md): UI meanings and current source fields.
4. [Implementation status](implementation-status.md): Figma versus code versus verification.
5. [Prototype links](prototype-links.md): current flows and preserved versions.
6. [Methodology requirements](methodology-requirements.md): content, calibration provenance, evidence and delivery checklist.
7. [User stories and onboarding](user-stories-onboarding.md): draft workflow, Saved stories and first-run flow (proposed, being worked out).
8. [Strength mock data](mock/strength-mock.md): real saved-export strength fixtures ready for Figma.
9. [Workflow design review](workflow-design-review.md): page map and Discover hierarchy proposal started 2026-10-07; standouts remain pending review.

## How to maintain this memory

- These files record intent and status; source code and inspected payloads establish actual capabilities. Figma establishes the visual design, not a working backend.
- Follow the user's latest explicit decision. If code, Figma and these documents disagree, state the discrepancy rather than silently choosing one.
- Update only the affected documents after work. Include date, scope, source links and test results where relevant.
- Use decision states: proposed, agreed, superseded. Track delivery separately: pending, Figma-only, code implemented, verified, deployed (only with evidence).
- Preserve the reason for a changed decision. Do not erase earlier direction or mark pending items complete because they were discussed.
- This is a baseline from the design conversation and local source inspection. Figma status below reflects the last verified edits in that conversation, not a fresh remote audit on 2026-09-29.
- Do not store credentials, environment-file contents or private connection strings here.

## Immediate handoff

2026-10-12: Desktop Match details has a first Figma review: [layout, carry-over rules and remaining verification checklist](desktop-match-details-todo.md). Frontend implementation remains pending.

2026-10-07: **Biggest standouts design is pending review** (D82). Existing Discover, full-list and detail prototypes are preserved; Figma delivery does not imply design approval.

2026-10-04 update: the owner requested the real Forecast record page. It is now
implemented locally in the original Next.js frontend at `/forecast-record`, with
typed, authenticated adapters to the Ballistics API. Scoped type/contract tests
and isolated backend tests passed. Browser interactions and desktop/mobile visual
checks now pass with explicitly synthetic intercepted test fixtures; live
configuration, authenticated end-to-end smoke testing and deployment remain pending.
See implementation-status.md for the resolved startup limitation.
This advances only the Forecast record page beyond
the earlier design-first baseline; other frontend scope is unchanged.

Updated 2026-10-02 (docs reconciled against the decision and status logs).

Done in Figma: D06 (league averages removed from Discover cards, kept in details), Methodology access (How it works footer plus contextual links, D31), Forecast record in primary navigation (D34).

Open:
1. **Saved** has no screen yet. Model agreed (D37–D38: fixture shortlist + followed leagues, account from day one); stories in [user-stories-onboarding.md](user-stories-onboarding.md). Navigation placement still open.
2. **Onboarding / first run** is not designed. Draft flow in the same doc.
3. **Strength Discover** uses the engine shortlist at gap > 250 (D43/D44, engine updated). Mock data: [mock/strength-mock.md](mock/strength-mock.md).
5. **Desktop** onboarding, teaser, Saved and Account built (D50). Pricing still undecided ("Price to be announced").
4. **Forecast record backend** (`forecast_history.py`) is deliberately not deployed: new and still in development (D35).

The earlier design-first baseline is superseded only for the locally implemented
Forecast record page described above. No deployment is claimed.
