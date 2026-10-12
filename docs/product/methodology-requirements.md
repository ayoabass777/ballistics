# Methodology — content and implementation requirements

Date: 2026-09-29. Status: requirements and draft copy, not an implemented page.

2026-10-01 update: revised desktop/mobile Figma screens now exist (overview plus seven topics), with contextual links from 112 saved-data detail states and a mobile-browser-tested return path. See prototype-links.md. This supersedes the earlier Figma-pending status only; source-definition verification, provenance fields, evaluation evidence, persistent navigation approval, accessibility implementation and frontend delivery remain pending. Content deliberately labels unknown calibration status and unfinished performance evidence. Technical model names and numeric strength thresholds appear only in the optional technical topic.

Update: Brooklyn and Detroit Overall run labels were subsequently verified from cutoff-filtered DB event history and now show 11 and 12, not 10+. See data-contract.md for provenance. The legacy-cap explanation below applies only to records not yet recomputed; exact verified counts should be preferred.

## Purpose and agreed boundary

Explain what the numbers mean, where they come from, how they are evaluated and where they can fail. Lead with football language; technical model names belong in optional detail, never in a model picker. Calibration provenance belongs here, not as a Global badge in match details. Projected home/away goals stay out of the main experience.

The 52 calibration rows in current saved-data Figma detail states were hidden on this date. Original design page is preserved. Methodology navigation, revised Figma screen and frontend remain to be built; this document does not claim those are complete.

## Proposed page structure

Use a short introduction and independently expandable sections, with one readable section at a time on mobile. Keep section headings linkable for contextual help. Persistent navigation placement remains a proposal; do not silently treat it as agreed.

1. **Reading the numbers:** observed records versus future estimates versus rankings.
2. **Patterns and league comparisons:** event definitions, sample and match scope.
3. **How goal estimates are produced:** inputs, probability meaning and limitations.
4. **How estimates are checked and adjusted:** calibration and supporting evaluation.
5. **Data coverage and freshness:** included competitions, missing fields and update times.
6. **Forecast record:** link to timestamped historical evidence when implemented.
7. **Technical details and changes:** versions, assumptions, calibration status and changelog.

### Reading the numbers

Draft copy: “A season record describes what happened. An estimated chance describes what may happen next. A ranking compares a team with others in its league. These are different measures and should not be read as interchangeable.”

Explain hit/total and observed percentage with a clearly identified example; never label a historical frequency a forecast. Explain that a high probability is not certainty and multiple related goal markets are not independent confirmations.

### Patterns and league comparisons

- Define each supported event in football language, including whether a goals threshold refers to the team or total match goals. State full-time/extra-time rules only after verifying settlement logic.
- Explain current-season coverage and At home, Away and Overall. Overall uses source context=all, not a guessed sum of displayed counts.
- Explain that patterns can differ between teams and scopes. The pattern scope is independent of the attack/defence comparison scope.
- Define league average as the same event/context baseline. Do not invent a league sample denominator: the inspected export does not supply it.
- Define current run as consecutive qualifying matches ending at the latest included result. Distinguish it from season hit/total. Explain 10+ on old capped exports until refreshed output is used.
- Explain discovery selection separately from the historical records available in details. The prototype's four selected records per team are not a complete or validated ranking.
- Document small-sample rules and any overall-context fallback from actual implementation, with the threshold/version. Do not imply eligibility means statistical certainty.

### Team context and goal estimates

- Goals scored/conceded per match are observed rates, not forecast goals. Explain selected split and season.
- Attack/defence ranks must state league population, scope, direction (#1 best), calculation and tie rules. Verify the ranking implementation before publishing a formula or claiming it ranks the displayed raw rate directly.
- Product goal estimates use Dixon–Coles under the agreed design. Keep that name in optional technical detail; explain the public-facing purpose first.
- Explain event-specific probabilities and supplied/unavailable forecasts. Missing output is not 0%.
- Do not expose projected home/away goals as predicted scores. Retain internal lambda fields for diagnostics; public technical disclosure of individual values is not required by this brief.
- Document actual input coverage; do not claim lineup, injury, corners or cards inputs without source evidence.

## Calibration section

### Draft plain-language copy

“We compare earlier estimates with what actually happened. Where a suitable adjustment is available, we use that evidence to adjust future probabilities. The aim is for events estimated around 70% to happen roughly seven times in ten across many comparable forecasts—not to identify which individual match will succeed. This is an aim, not a claim that our current estimates have achieved it.”

“A shared adjustment uses historical evidence pooled across included leagues. Some supported markets may also have a league-specific adjustment. Neither approach guarantees accuracy, and performance can change over time.”

### Verified code behaviour and current caveat

- `engine/eval/predict_dixon_coles.py::_calibrate` uses available calibration-map points; if no map is available it returns the raw probability. The Goals 1–4 market can use a dedicated map, otherwise the general DC map, plus an available league adjustment.
- The predictor writes calibration_scope=global+league when a league adjustment is present, otherwise global. Therefore global alone does **not** prove calibration was applied.
- `engine/eval/calibration_maps.py` loads shared maps and optional league adjustments. Fitting uses equal-count bins and a monotonic PAVA mapping; optional league offsets are shrunk toward the global expectation. Describe technical mechanics only in the advanced section.
- `engine/eval/consensus_slips.py` carries raw/global/final values and scope into exports. Existing fields do not establish map identity, fit dates or evaluation quality.
- Do not claim a specific saved prediction was calibrated with a particular map merely because a map exists now. Historical provenance must be tied to that prediction's generation.

### Required provenance contract — proposed, not implemented

Separate application status from evaluation status. Record per forecast or resolvable forecast run:

| Field | Required meaning |
| --- | --- |
| Forecast ID, model version, market, league and run ID | Stable identity for reproducing the displayed result |
| Source-data cutoff and forecast generation time | Separate UTC timestamps; export generation is not the data cutoff |
| Raw probability and displayed probability | Internal trace from model output to presented estimate |
| Calibration application status | applied / not_applied / unknown; never infer from global alone |
| Calibration scope | shared / shared_plus_league / not_applicable / unknown |
| Map ID/version, calibration key and fit cutoff | Exact adjustment artifact used; include dedicated-map fallback path |
| League adjustment status and version | Whether applied, unavailable, unsupported or unknown |
| Fallback reason | Missing, invalid or incompatible artifact, unsupported market, insufficient data, or other explicit reason |
| Evaluation reference and status | Link to matching evidence, or clearly not evaluated/unknown; applying a map is not proof of improvement |

Suggested public states: “Adjusted using shared historical evidence”, “Shared adjustment with league adjustment”, “No calibration adjustment applied”, and “Calibration status unavailable”. Only show the first two when recorded provenance supports them. State sample limitations alongside evidence, not as a hidden footnote.

## Evidence needed before publishing performance claims

- Use temporally held-out/walk-forward evaluation. Each forecast, model fit and calibration fit must use only information available at its cutoff; evaluate calibration on data not used to fit that map.
- Record evaluation dates, market, league coverage, sample counts, exclusions, versions and any selection threshold.
- Compare raw and adjusted probabilities on the same held-out sample. Proposed evidence: reliability bins with counts and uncertainty, Brier score and log loss, plus an appropriate baseline. These are requirements, not results established in this design work.
- Separate full forecast population from Discover-selected forecasts and separate markets/leagues where sample sizes permit. Do not pool incompatible measures into an overall accuracy badge.
- Show weak, negative or insufficient evidence honestly. Publish no guaranteed-win, profitability or calibrated-accuracy claim without supporting evidence. Hit rate alone is not profitability.
- Link to an immutable, pre-match forecast record with outcome/settlement status, including losses and voids. Prevent hindsight edits; version later corrections. Define postponed, abandoned, duplicate and unsettled fixtures before launch.

## Freshness, limitations and changes

Explain source coverage, timezone, missing/late data and correction policy. Current mock is the Sep 28 run exported Sep 29 at 00:08 UTC, not live data. Verify refresh promises against deployed jobs before publishing them. Separate season boundaries, source cutoff, model run, calibration fit and page update dates.

Include unsupported inputs, changing team conditions, sparse/new-team history and correlated markets where applicable. Make absence of evidence visible; do not invent uncertainty intervals. Maintain a dated methodology changelog and preserve prior versions so old forecasts remain interpretable.

## Delivery checklist

- [x] Remove Calibration: Global from the current saved-data Figma match details, including linked dropdown states.
- [x] Capture required content, draft calibration copy and the provenance caveat in this document.
- [x] Build revised desktop/mobile Methodology screens; preserve the original design. Figma-only, 2026-10-01.
- [x] Add contextual “How these numbers work” links and a persistent entry point (How it works footer, D31). Figma only; return-to-origin wired and mobile browser-tested. Frontend still pending.
- [ ] Verify event, rank and settlement definitions against code; resolve undocumented behaviour.
- [ ] Implement explicit calibration application/fallback metadata; do not change probability calculations as part of UI copy work.
- [ ] Attach version-matched held-out evaluation evidence; otherwise show evidence unavailable.
- [ ] Test applied shared, applied league-adjusted, missing-map/raw fallback, legacy unknown status, missing forecast, sparse sample and stale-data states.
- [ ] Test mobile disclosure controls, keyboard interaction, readable labels and screen-reader state announcements.
- [ ] Confirm projected goals and calibration labels remain absent from every match-detail state; retain estimate/historical distinctions and limitations.
- [ ] Implement and verify the frontend separately. No deployment or live-data integration is claimed here.
