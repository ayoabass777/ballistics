# UI data contract

2026-10-12 — D116 onboarding adds only session-local visibility state for a first-detail tip. Existing saved fixture evidence unchanged. No account persistence or production first-visit tracking added.

2026-10-11 — D112 teaser samples: Radnicki detail clones D107 frozen evidence unchanged. Fortaleza/Nautico uses D53 saved 1–4 goals estimate 83.2% and league rate 84.4% (difference −1.2 percentage points); Cuiaba/Ponte Preta uses D53 observed 14/15 away losses and league rate 45.0%. Dates and sample status are visible. Brazilian previews explicitly limit coverage to supplied findings; no additional evidence or current forecasts inferred. Save opens membership, with no save action performed.

2026-10-09 — D107 first-fixture v2 sample: Radnicki 1923/FK Crvena Zvezda, fixture 126487, retains frozen Oct 10 source. Context rates 1.89/1.00 vs 2.40/0.30 are all-venue season averages. Patterns pair unbeaten 7/9 vs 10/10 and under-4.5 7/9 vs 9/10, with additional home under-3.5 and away scored-1+ records. Goals retain 78%, 78%, 76%. Missing league counts/rates, current runs, venue splits, standings/H2H and ranks stay explicitly unavailable; no unrelated fixture values substituted. The Big dog label is inherited from the saved shortlist, not recalculated. Shared Save remains prototype-session only.

2026-10-08 — D105 established-layout integration uses the frozen 80-fixture October sample. Pattern team tabs select supplied records; missing league comparisons, consecutive runs, venue splits and ranks are explicitly unavailable. The sample supports one supplied pattern scope and overall team averages, so scope controls do not promise unavailable data. Goals retain stored estimates/comparison wording. Standings/H2H are unavailable in this particular sample, not asserted absent in all product sources. Save remains shared prototype-session state. No account persistence or live data added; separate v2 contract remains intact.

2026-10-07 — Match detail v2 contract: see [match-detail-contract.md](match-detail-contract.md) — field-by-field sources for Context (venue split, standings, H2H), Strength, Patterns and Goals, label wording, entry behaviour, missing-data rules and the gaps to close before build (W/D/L, team IDs in export, pattern league sample size, match endpoint).

2026-10-07 — D98: Prototype Save coverage expands to all 80 fixture identities in the frozen standout sample. Each shares saved/unsaved booleans across current Discover, full list, details and one Saved card. Saved retains frozen source values and date; no timestamp capture, live update, cross-device persistence or backend implementation claimed.

2026-10-07 — D93 Save prototype maps each fixture to one saved/unsaved boolean pair, shared across placements. Saved displays frozen source-card values for the selected fixture; it does not record an actual save-time data version or timestamp. Counts, results, evolving estimates, cross-device storage and backend persistence remain separate implementation work. 29 fixture identities deduplicated from the D89 samples.

2026-10-07 — D91 intended Discover scope: upcoming fixtures from now through the end of the sixth calendar day after today in the user timezone (exclusive start of day seven); seven calendar dates, not 168 hours. Apply league scope before ranking within this window for standouts and category lists. Render local fixture date/time in implementation. Current Figma displays fixed 7–13 Oct label with original 10 Oct UTC examples only; no export regeneration or engine changes.

2026-10-07 — D89 pagination sample: nine Strength and ten each Goals/Patterns drawn in source order from frozen D79/D80 standout population. Counts explicitly say sample fixtures; these are bounded layout samples, not complete category totals or current engine eligibility. Prototype initial batch five; expansion reveals remaining sample. Production totals must come from selected category/date/league population.

2026-10-07 — Longer Team strength layout test 590:54448 uses all nine Strength cards in the frozen D79/D80 standout list. These illustrate list length, not complete current Team strength eligibility or ranking; includes historical candidates predating D81. No source values or engine rules changed.

2026-10-07 — Frame 588:54432 uses an explicitly illustrative Borussia Mönchengladbach / Wolverhampton Wanderers pairing solely to test long names and description wrapping. No real fixture, strength classification or forecast is asserted; first-card match navigation disabled. Remaining sample content comes from the prior frozen preview.

2026-10-07 — D88 carousel reuses the first five fixtures from the frozen D79/D80 ranking in order: 126487, 60892, 81893, 79909, 71009. All five happen to be Strength findings in this snapshot; no category quotas introduced. Fixture identities, UTC times and destinations preserved. Five is the carousel preview count, while all 80 remain in the existing full list; no engine selection change or fresh D81 run is claimed.

2026-10-07 — D83 preview provenance: reuses nine cards from D79/D80 frozen 10 October standout list, three per category, with original fixture identities, findings, values and detail destinations. These are layout samples selected from the standout population, not complete independent category rankings. Category labels omit original cross-category rank numbers. All leagues and UTC are explicit fixed sample scope. The 80 total belongs to the original frozen standout snapshot, not a new engine run or D81 recomputation.

2026-10-06 — D81 Big dog leagues: `elo_gap_group.by_league[]` now covers every league with ≥ 50 rated matches (no upset-rate pre-filter), so the pooled `won`/`matches`/`win_rate` and `max_league_upset_rate` (now 1.0) describe all leagues, not the old 30. New `elo_gap_group.big_dog_leagues[]` {league, country, tier, status: in/out, reason, held, was_in, matches, won, win_rate, win_rate_low}: the authoritative list of leagues allowed Big dogs this run. `held: true` means kept by the buffer (low between 57% and 60%). `elo_gap_shortlist[]` contains only fixtures from `status: in` leagues; `league_upset_rate` stays as context only and no longer gates. `league_record_ok` = league is `in`. Membership persists in `engine/eval/big_dog_leagues_state.json` (not part of the export).

2026-10-06 — D80 detail preview: [route manifest](mock/standout-detail-routes-2026-10-06.json) maps 80 fixture IDs to desktop/mobile overview destinations. Values come from the frozen D78 source export and selected standout manifest. Goals overview shows up to three highest supplied non-win probabilities plus qualitative league comparisons; selected standout remains separate. Supporting historical rows use primary contexts, n≥6, up to three highest observed rates per side, excluding halftime_win; this summary does not implement the complete D73 redundancy policy. Season rates use overall attack_rate/defense_rate. No invented standings/H2H. Identity rows preserve fixture home/away order and exact asset-name matching; initials used where asset missing.

2026-10-06 — D79 Figma source: [manifest](mock/standouts-figma-2026-10-06.json) records all 80 fixture IDs and ranking order for 10 October from the frozen D78 experiment. Scores derive from current engine replay with prior15, no historical display_eligible gate, inclusive adjusted score ≥0.15, no quotas/count cap. Label estimates separately from historical counts. Prototype is fixed to this date; existing Discover league scope controls do not recompute this new snapshot. Other Discover sections retain older saved examples. New standout cards have no match-detail routes; only full-list navigation is implemented.

2026-10-06 — D78: discover_pick is null unless the best eligible score is at least 0.15 (15 adjusted percentage points); compare unrounded values, allowing only floating-point arithmetic noise. Exported score remains rounded to four decimals. This is not a 15% predicted chance or relative percentage uplift. Per-section candidates and strength labels are not suppressed by this standalone cutoff. No section quota or result-count cap.

2026-10-06 — D77 supersedes D71 pattern score prior: score=(k/n−base_rate)×n/(n+15). Historical standout eligibility requires n≥6 and a supplied valid league comparison, not display_eligible. Preserve observed k/n and model prediction metadata independently; score is not a forecast probability. Cutoffs under review are inclusive adjusted percentage-point differences (5/10/15), not raw rate differences. Existing ranking still considers supplied primary/secondary contexts and half-time events; D73's narrower match-detail trial rules have not been propagated.

2026-10-06 — D76 crests: 26 API-Sports team PNGs mapped using the saved verified-match-context fixture IDs, downloaded and uploaded on this date to Figma asset frame 519:54384. Source: https://media.api-sports.io/football/teams/{api_team_id}.png. Preserve aspect ratio, equal bounds and visible names; crest size does not encode strength. Static Figma assets only, not live API wiring or independently verified season branding. All current examples have assets; initials fallback remains pending frontend work.

2026-10-06 — D75 logo trial: API-Sports public league PNGs for IDs 345 (Czech Liga), 255 (USL Championship), 489 (USL League One); FlagCDN w80 PNGs for br/us/gb-eng/es/ng/ke/tz. Eleven FIT image placements, downloaded and uploaded on this date. Preserve visible names and aspect ratios. Provider branding is not independently verified for the displayed season; assets are Figma samples, not a live asset contract or rollout.

2026-10-05 — D73 historical trial: `mock/historical-pattern-preview-2026-10-05.json` records selected and suppressed events plus complete source rows from the Oct 2 export. Reproduce with `mock/build_historical_pattern_preview.py`. Kairat primary context home, Barcelona all; no Bayesian rating gate, no changes to source eligibility flags. Display actual k/n, streak and context-matched qualitative baseline comparison. Minimum n=6 and maximum six records are preview assumptions, not validated production gates. League sample counts are missing in this legacy source. Historical comparison is descriptive, not forecast uplift. No live adapter change.

2026-10-05 — D72 agreed: retain the Bayesian belief prior weight of 15; do not use n/2. Historical hit/total remains an observed record, separate from the smoothed rating. Discover ranking shrinkage is a separate calculation governed by D71.

2026-10-05 — Raw Bayesian smoothing evidence: [prior comparison](experiments/bayesian-prior-2026-10-05/report.md) supports retaining 15 over tested weaker fixed weights and n/2 for raw predictive error. Does not establish an optimal prior or validate the final calibrated/boosted rating. Historical k/n remains unchanged by smoothing. The 75% display gate and historical-pattern visibility are separate product concerns; no contract gate changed in this experiment.

2026-10-05 — D71 Discover pick (engine code, not shipped): every `fixture_analysis[]` entry gains `discover_pick`, or null when nothing scores above 0. Shared fields: `list` (strength / goals / patterns), `score` (points above league, 0–1 scale), `big_dog` (favourite name or null), `runner_up` {list, score}, `candidates`. Goals adds `market`, `probability`, `league_rate`, `league_comparison`. Patterns adds `team`, `event`, `context`, `k`, `n`, `base_rate`, `shrunk_rate`, `league_comparison`. Strength adds `favourite`, `underdog`, `elo_gap`, `group_win_rate`, `group_rate_source` ("band 300-400" / "all big dogs"), `league_win_rate`, `league_win_context` (home / away / all). `elo_gap_shortlist[]` gains the same strength fields plus `discover_score`. `elo_gap_group` gains `bands[]` {band, min, max, matches, won, drew, lost, win_rate} and `by_league[]` {league, country, matches, won, drew, lost, win_rate, win_rate_low, meets_min}. `elo_gap_shortlist[]` also gains `league_big_dog_matches`, `league_big_dog_won`, `league_big_dog_win_rate`, `league_big_dog_win_rate_low`, `league_record_ok` (true only when the league passes D74: ≥ 30 past Big dog matches and lower bound ≥ 60%); strength `group_win_rate` is now the league's lower bound, and `group_rate_source` reads e.g. "league record 160/200 (lower bound)". `score`, `shrunk_rate` and group rates rank cards only and are never shown as chances (D61 still applies to visible copy).

2026-10-05 — D70 crest preview assets: public API-Sports PNGs at `https://media.api-sports.io/football/teams/{id}.png`: Zlin 603, Slavia Praha 560, Fortaleza EC 154, Nautico Recife 755, Cuiaba 1193, Ponte Preta 139. Uploaded to Figma on this date; not live asset integration. Preserve aspect ratio (FIT), equal bounding boxes and visible names. Crest size does not encode team strength. Initials fallback remains proposed, not implemented in this populated visual preview.

2026-10-05 — D69 evidence: `mock/verified-match-context-2026-10-05.json` contains 13 fixtures from read-only local `raw.raw_fixtures` and `raw.raw_league_standings`. H2H matches both team IDs in either venue order, FT only, before min(fixture kickoff, export cutoff), newest five available. Export cutoffs: 2026-09-29 00:08 UTC and 2026-10-03 00:07 UTC. Coverage is not exhaustive. Standings is a current database snapshot, not point-in-time forecast evidence; display season and `standings_updated_at`. `display_standings` uses the latest refresh cohort; raw rows and stale-row count remain for audit. Missing conference groups plus repeated ranks suppress the USL Championship table. No live API integration added.

2026-10-05 — D67 Figma comparison conversion: 536 visible pattern baseline labels converted from existing displayed league rates and adjacent k/n. Gap is 100*k/n minus the displayed league percentage; >=15 points much more, >=5 more, >-5 typical, >-15 less, otherwise much less; n<6 gives too few matches. These are descriptive comparisons using rounded saved baseline values, not new probabilities or statistical significance claims. League sample counts remain unavailable in legacy mocks; engine-level minimum league sample validation is not established by this copy change. Source data and observed percentages retained.

2026-10-05 — D66 presentation: standings data requirements are unchanged; dedicated page must retain league/season and originating fixture context. Current review displays Czech Liga and Zlin/Slavia with saved-run date, but no verified standings season/ranks/points. Missing data remains explicit. H2H stays a separate inline disclosure.

2026-10-04 — D65 pending supporting-context data: fixture_analysis in the 2 Oct export has neither league standings nor previous-meeting records. Do not interpret missing data as zero points or no previous meetings. Proposed standings contract needs league/season/group, snapshot cutoff, team identity, rank, played, goal difference and points; highlight fixture teams once populated. Proposed H2H needs completed meeting identity, date, competition, home/away identities and final score, restricted to meetings before the snapshot cutoff. Current Figma expands to unavailable states only. These payload contracts are proposals, not engine fields.

2026-10-04 — D64 Strength-card presentation: headline identifies the existing saved Big dog team, not necessarily the home team. Qualitative copy names its rating advantage over the opponent; fixture order and kickoff remain unchanged below. No win probability, historical outcome split or new classification is inferred. The redesign preserves each prior card's navigation destination and underlying saved fixture identity.

2026-10-04 — Discover Goals comparison provenance: Sep 28 export, fixture 66404 Match Under 3.5 probability 0.8168 versus matching all-context league base_rate 0.6813 (gap +13.55 points); fixture 67719 Match Over 1.5 probability 0.7468 versus base_rate 0.8026 (gap −5.58 points). Figma uses qualitative more/less likely than usual wording, naming league and season. These are saved-data comparisons, not evidence of model superiority; the legacy export does not provide league sample counts for validating the new engine minimum-sample gate.

2026-10-04 — D61 presentation update (focused mobile Figma review only): strength labels and qualitative gap description remain visible. The saved numeric gap (501 for Zlin/Slavia in this example) and shortlist threshold (>250) appear only in the optional “How strength is measured” disclosure. No per-match win probability or illustrative W/D/L group record is displayed. `elo_gap_group` remains an engine field; its existence does not authorise presenting unverified historical results. Existing saved fixture data and model outputs are unchanged.

2026-10-04 real page adapter: `/forecast-record` in the original Next.js frontend
now consumes the ledger API through authenticated same-origin routes and runtime
response validation. No mock fallback. League directory comes from the new ledger
`/filters` route, not legacy navigation data. Each event has its own capture and
outcome history; mixed fixture outcomes are retained. UTC date filtering remains
explicit even though displayed timestamps use browser timezone. No true last-check
freshness badge, no pooled accuracy, no assumed three-event cardinality. Live
configuration/deployment and browser verification remain separate.

2026-10-03 local HTTP contract: `GET /api/forecast-record` supports inclusive UTC captured-kickoff dates, either-team literal search, exact league/country names and outcome filters; SQL paginates complete fixtures (25 default, 100 maximum). `totals` follows all filters, while `cohort.status_counts` is before outcome filtering. Mixed-outcome fixtures show only matching events under each tab. Reuse returned `as_of` across pages to freeze eligible selections and outcomes. Entry correction history is separately paginated. `selection_state` reflects the captured policy cutoff as of that browsing snapshot, not the match result. `outcome_observed_at` is the last changed observation, NOT last successful check/source freshness; unobserved results are explicit. Full contract and limitations: ../forecast_api.md. Local backend only; no frontend/Figma/deployment change. Supersedes earlier API-pending notes below.

2026-10-03 selection-policy amendment: latest eligible pre-kickoff capture replaces first-capture selection below. Active policy `latest_pre_kickoff_dc_goals_v1`; all versions remain immutable. Show “Latest captured forecast” before selection closes and “Last forecast before kickoff” afterward, with database capture time. Each event advances independently; late captures cannot replace it. Conservative reschedule cutoff, legacy policy isolation and details: ../forecast_history.md. Backend-only change; Figma/frontend have not been updated.

2026-10-03 volume prototype: 50 synthetic fixture IDs each have three illustrative goal forecasts (150 total); 32 fixtures settled, 12 pending, six other. Outcome tabs scope the fixture list and headline counts; the secondary status breakdown covers the chosen date/team/league population before the outcome tab. Pagination counts are for all matching records, not just loaded rows. Team search matches either side (Brooklyn returns two sample fixtures). Dates are kickoff dates in UTC, with 26 Sep–2 Oct and empty 19–25 Sep demo ranges. All estimates/results/capture times are synthetic, not verified history. Production must additionally handle fixtures containing mixed event statuses; do not assume every fixture always has exactly three forecasts or one shared status.

2026-10-02 strength source: label fixtures from `fixture_analysis[].mismatch[].gap` (raw Elo gap, no home advantage); the favourite is the team named in its mismatch markets. Fixtures with no mismatch record have an unknown gap, not "closely matched". Discover Strength uses the engine `elo_gap_shortlist` (D43/D44: gap above 250, low-upset leagues; engine updated 2026-10-02); D27 labels apply in match details. See [mock/strength-mock.md](mock/strength-mock.md).

2026-10-02 prospective record contract: engine/eval/forecast_history.py report emits policy, exported-DC-goal population, capture period, all status counts and original forecast/latest settlement records. recorded_at is database capture time, not run_ds or training cutoff; observed_at is outcome freshness. First eligible forecast stays selected across later runs. Pending, hit, miss, void, unavailable and excluded must remain visible; only hit/miss are settled binary outcomes. No blanket accuracy badge across correlated events. Existing daily JSON/results are legacy, not eligible historical imports. Frontend/API and verified model-training/calibration provenance remain pending; see ../forecast_history.md.

2026-09-30 Matches browsing: date is the UTC calendar day of kickoff in the saved-data prototype. Team query is combined with league using AND, matches either fixture side in the intended frontend, and does not silently change the selected date. The Figma example query is Brooklyn only. Missing fixture days in the saved sample are not proof of an empty live schedule. Clear filters retains date; Today retains filters and is fixed to 30 Sep 2026 for the demo. Arbitrary calendar dates and typed search require frontend implementation.

2026-09-30 strength mock: Naples Big dog / Richmond Kickers Underdog is user-authorized illustrative classification for layout review, not derived from validated rating thresholds. Keep its Mock example and unverified wording alongside the classification even though other fixture metrics are real saved data. Do not infer a win probability or a populated strength shortlist from this mock.

2026-09-30: Pattern mock now includes score_1goal, concede_1, loss and Under_4_5 for each side and venue/all context in fixtures 48565 and 67719. Exact k/n, calculated observed percentage and supplied base_rate used independently per record. For these new panels, legacy streak=10 is unavailable until verified (the export is capped); values below ten are preserved. Do not copy Brooklyn's corrected runs to other teams. Pattern scope remains independent of attack/defence scope.

2026-09-30 real-mock expansion: source remains consensus_slips_2026-09-28.json, generated 2026-09-29 00:08 UTC. Active Goals examples are fixture 66404 Match Under 3.5 (0.8168) and 67719 Match Over 1.5 (0.7468); use 81.7% and 74.7%. Botafogo 48565 has no DC forecasts. elo_gap_shortlist is empty despite some fixture-level mismatch market records; show a shortlist-empty state, not zero strength, equal teams or invented qualitative categories. Active Matches contains the three existing snapshot fixtures in chronological UTC order.

## Verified Overall run correction — 2026-09-29

For the Brooklyn/Detroit mock, replace the old capped export streaks with counts recomputed from the current database at the mock cutoff: Brooklyn concede_1 = 11; Detroit City Under_4_5 = 12. Read-only query of raw.fact_head_to_head_events, h2h_context_scope=all, is_current, kickoff_utc before 2026-09-29 00:08 UTC; league_season_id=833; team IDs 15154 and 15054. Sorted by kickoff_utc then fixture_id, count trailing event_flag=1. Brooklyn has 22/25 hits; last miss fixture 66250 (July 4), followed by 11 hits through fixture 66391 (Sep 23). Detroit has 24/25 hits; last miss fixture 66222 (June 13), followed by 12 hits through fixture 66396 (Sep 26). Both totals match the saved export. This is cutoff-filtered reconstruction from current DB contents, not an archived point-in-time DB snapshot. No database writes or export regeneration performed. Other legacy capped records remain unverified.

Calibration placement (2026-09-29): remove the scope badge from match details. Methodology owns the explanation and technical provenance. See methodology-requirements.md for proposed explicit application/fallback status, map/version identifiers and evaluation evidence. Existing calibration_scope alone is insufficient; this is a pending metadata requirement, not a schema change performed here.

Projected-goals presentation (2026-09-29): lambda_home/lambda_away remain internal source fields but are removed from the main prototype. Calibration scope is provenance, not a quality score. predict_dixon_coles.py labels global when no league adjustment is present; _calibrate applies the global map when available but returns raw probability if maps are missing. Consequently global alone does not prove an adjustment occurred or that accuracy was validated. Keep any stronger calibration claims tied to actual map/version and evaluation evidence.

Overall-pattern extension (2026-09-29): the pattern panel now has its own scope selector. Overall maps to the selected team's `bayesian` records with `context=all`, not a sum of venue records. Refresh k/n, base_rate and streak together. A saved streak of ten is shown as 10+ because this export predates the cap fix. The independent attack/defence scope does not change pattern scope. Earlier venue-only exploration below is preserved as history.

Team-pattern exploration (2026-09-29): group records by fixture side/team and retain actual context. Brooklyn uses primary home records; Detroit City uses primary away records. Four records per team are selected to demonstrate different events, not a complete/ranked list. Some are below the existing display threshold: label as historical context, not discovery recommendations. Patterns remain venue-specific when the independent attack/defence comparison switches to Overall.

Baseline recorded: 2026-09-29. This is a mapping of inspected local output, not a guarantee of live production coverage or forecast accuracy.

## Controlling sources

### Both-team comparison (prototype added 2026-09-29)

Use both `home_team` and `away_team` strength profiles, independently of the selected pattern. Home/Away selects `home_team.home` and `away_team.away`; Overall selects both `.overall` profiles. Map `attack_rate` to goals scored/match, `defense_rate` to goals conceded/match, `attack_rank` and `defense_rank` to ranks with denominator `league_size`. Do not substitute adjusted/shrunk rates or imply these observed rates are forecasts. Rank #1 is best; ranks use the selected split. Scope toggle affects this comparison only, not the separately labelled pattern evidence.

- Existing fixture-analysis UI: `engine/eval/consensus_app.html`, especially `renderAnalysis` and `renderStrengthPanel`.
- Saved mock source: `engine/eval/consensus_slips_2026-09-28.json`, generated `2026-09-29 00:08 UTC`, run date `2026-09-28`.
- Payload assembly: `engine/eval/consensus_slips.py`.
- Pattern definitions: `engine/eval/bayesian_streaks.py`, particularly `compute_beliefs`, `compute_league_base_rates` and `get_fixture_beliefs`.
- Attack/defence profiles: `engine/eval/team_strength.py`.

## Field meanings and presentation

| UI concept | Current field / derivation | Rules |
| --- | --- | --- |
| Fixture identity | `fixture_analysis[].fixture_id`, `home`, `away`, `league`, `country`, `kickoff` | Preserve fixture identity across cards and detail routes. Include timezone. |
| Pattern event | `bayesian[].event`, `team`, `side`, `context` | Translate to football language. `side` is fixture side; `context` is the record's home/away/all split. |
| Season hit/total | `k / n` | Current-season matches in that context, not automatically the last ten. |
| Misses | `n - k` | Derive only from present, valid counts. |
| Observed rate | `k / n` when `n > 0` | Historical frequency, not next-match probability. |
| League average | `base_rate` | Observed current-season event baseline for the same league/context. Detail view only under D06. |
| Difference from league | `(k/n - base_rate) * 100` | If used, label percentage points, not percentage improvement. |
| Consecutive run | Export `streak`; computed `recent_streak` | Full trailing run within loaded season/context after the fix. Detail view only. Older capped exports require “10+” for a saved ten unless recomputed. |
| Pattern rating / estimate | `posterior` | Current output combines calibration and a capped streak boost; not raw hit rate. Preserve provenance internally. |
| Estimate provenance | `posterior_raw`, `posterior_calibrated`, `boost`, `alpha`, `beta` | Technical detail, not a main-screen model selector. Do not treat raw distribution parameters as uncertainty for the final boosted estimate without further work. |
| Attack / defence | `strength.home_team` / `away_team`, then `overall`, `home`, `away` | `attack_rate` and `defense_rate` are observed goals per game; `*_shrunk` are adjusted rates; `*_rank`, `*_pct`, `league_size` provide league context. |
| Goal forecast | `dixon_coles[].market`, `probability` | DC-only product forecast direction. An empty array means no supplied forecast, not zero probability. |
| Projected goals | `lambda_home`, `lambda_away` in DC output | Forecast goal rates, not observed goals or shot-based xG. |
| Calibration provenance | DC `raw`, `calibration_scope`, `global_calibrated`, `league_adjustment` | Preserve distinction from the presented calibrated probability. |
| Strength-gap signals | `mismatch[].gap`, `market`, `probability` | Availability varies. An empty signal list does not establish equal team strength. |
| Eligibility | `gates`, `display_eligible`, `display_threshold`, `consensus_eligible`, `eligibility_reason` | Display eligibility and inclusion in a selection pool are different concepts. |

## Important boundaries

- The inspected UI shows pattern beliefs at a default 75% display threshold; the saved payload also includes below-threshold beliefs. Do not confuse a display filter with absent data.
- Venue context is preferred when its sample is mature (currently at least six matches); otherwise the engine may select overall context. Label the actual context supplied.
- League baseline sample size is calculated upstream but is not carried in the inspected pattern export. Never invent “100 league matches” or another denominator.
- `generated` is export generation time, not necessarily the source-data cutoff or forecast-run time.
- No corners, cards, lineups or injury fields were established in this fixture-analysis payload. This does not assert they are absent from every other project source.
- Forecast record and Saved need separate implementation contracts; do not infer a working product feature from an evaluation script or Figma frame.

## Current mock fixtures

| Fixture ID | Fixture | Selected pattern | Season record | League baseline | Saved streak |
| --- | --- | --- | --- | --- | --- |
| 48565 | Botafogo SP vs Ponte Preta | Botafogo SP scored at least once, home | 13/14 | 75.84% | 7 |
| 66404 | Brooklyn vs Detroit City | Brooklyn conceded at least once, home | 10/12 | 67.81% | 8 |
| 67719 | Naples vs Richmond Kickers | Richmond Kickers lost, away | 12/14 | 51.07% | 10, displayed as 10+ |

The three examples are ordered by their saved selected-pattern ratings, not a demonstrated full-population ranking. Botafogo's selected snapshot has no DC output; its detail screen intentionally shows an unavailable forecast state. Keep this mock source fixed until intentionally replacing it, and update provenance when doing so.
