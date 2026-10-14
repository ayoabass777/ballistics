# Mobile and desktop prototype checkpoint — 13 October 2026

Status: Figma-only design baseline. Requested by the owner before the data-contract review.

## Preservation scope

13 editable screen copies named `CHECKPOINT · 2026-10-13` are preserved on page `16:353` of Figma file `dLgWAaafmzxQNcp2Rb9P7u`. The active screens are unchanged. 66 reactions targeting included nodes were remapped to checkpoint copies. Other destinations remain external to this checkpoint.

This is a frame checkpoint, **not an immutable Figma version-history backup or a fully isolated end-to-end prototype**. Components and variables remain shared. It does not checkpoint every Saved, Account, onboarding, calendar, overlay or legacy screen. Current saved sample data and known mismatches are deliberately preserved.

| Screen | Source | Checkpoint |
|---|---|---|
| Mobile Discover / Strength | 1169:62199 | 1299:62181 |
| Mobile Discover / Goals | 1169:62532 | 1299:62532 |
| Mobile Discover / Patterns | 1169:62865 | 1299:62883 |
| Mobile All standouts | 1169:63202 | 1299:63238 |
| Mobile Matches | 1155:62115 | 1299:65006 |
| Mobile Match details | 1196:62044 | 1299:65176 |
| Mobile Teaser | 1030:57675 | 1299:65526 |
| Desktop Discover / Strength | 16:354 | 1299:65653 |
| Desktop Discover / Goals | 22:452 | 1299:66604 |
| Desktop Discover / Patterns | 22:547 | 1299:67463 |
| Desktop Match details / 1440 | 1226:62085 | 1299:68345 |
| Desktop Match details / 1280 | 1235:62127 | 1299:68749 |
| Desktop Match details / 1024 | 1235:62521 | 1299:69153 |

[Mobile checkpoint](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1299-62181&starting-point-node-id=1299%3A62181) · [Desktop checkpoint](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1299-65653&starting-point-node-id=1299%3A65653)

## Review findings and recommendations

- **Fix before end-to-end handoff:** reconcile fixture identity and navigation. The Radnicki / Crvena card still targets a detail screen populated with Atromitos / AEK. Match identifiers should control every link; the contract should explicitly preserve fixture identity across cards and detail payloads.
- **Complete desktop routes:** some fixture, All standouts, calendar and account actions still lead to mobile screens. Track separately from visual approval.
- **Unify desktop navigation:** the simplified sidebar is on Discover only; Match details still shows Workspace, outlined inactive items and the old helper copy. Apply the agreed sidebar across active desktop screens in a later consistency pass.
- **Optional desktop usability improvement:** add previous/next controls for the Standouts carousel so mouse users need not discover horizontal scrolling. Keep swipe/trackpad scrolling.
- **Verification still required:** latest compact card action rows, show-more states and all desktop Discover widths need final interaction/responsive coverage. Existing breakpoint QA covers Match details, not the newly compacted Discover grid.

No major layout redesign recommended. Proceed to the data contract with these issues tracked; the design checkpoint is not a claim of complete navigation or frontend implementation.

## Data-contract handoff priorities

1. Fixture identity, team IDs and league-season IDs across every surface.
2. Date/timezone scope, Home/Away versus All venues, and source freshness.
3. Strength classification and the provenance/eligibility of the league-history claim.
4. Pattern eligibility using the existing Bayes gate, prominence using distance from league average, Results and Attack/defence pairing, deduplication and expandable-list ordering. Do not restore the rejected probability-above-league gate.
5. Forecast estimates versus observed records, sample denominators, missing/unavailable states and league baselines.

Verification on 2026-10-13: inspected current mobile Discover, mobile Match details and desktop Match details screenshots; used the previously verified desktop Discover screenshot. Checked cloned mobile/desktop Discover screenshots after checkpointing. Tool returned all 13 root mappings and 66 remapped reactions. No fresh end-to-end browser regression or production tests claimed.

## Follow-up — 14 October 2026

2026-10-14 — Fixture-link repair: 44 fixture actions across desktop Discover roots `16:354`, `22:452`, `22:547` now target the 29 matching existing 760px desktop saved-data previews listed in `mock/standout-detail-routes-2026-10-06.json`. This intentionally uses older saved-preview layouts, not the latest 1440px AEK review; migration to the current detail design remains pending. Nine Radnicki/Crvena links across mobile Discover (canonical and date-scope), All standouts and compact Matches roots now open matching mobile preview `542:54144`, rather than AEK `841:57932`. Removed incompatible section-scroll actions on retargeted links. Saved-preview Back labels made generic and top padding increased from 24 to 72px to avoid the Figma toolbar intercept. Browser verified desktop Discover → Radnicki → Back and mobile Discover → Radnicki → Back. Destination team identity checked against saved route manifest; 44 desktop fixture actions structurally verified without mobile destinations. Remaining mobile links were inspected by destination identity but not exhaustively clicked. Checkpoint copies and archives were excluded from route edits; their external destinations remain shared. No backend/frontend/data-contract changes. Calendar, account, All standouts navigation and other desktop layout migrations remain separate.

