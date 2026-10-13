# Two-stage pattern review — 2026-10-12

Status: rejected by owner 2026-10-12. Retained as trial history; not an active product rule.

Source: Oct 12 consensus export, generated Oct 13 00:35 UTC. Full figures and selected rows: [evidence](mock/patterns-two-stage-cross-fixture-check.json).

All-venues check across 559 fixtures, using unique side/event/context records, display_eligible, n>=6, k>0 and excluding half-time events. 545 fixtures have eligible records. At an 80% probability-above-fixed-baseline threshold, 96 of those have no prominent candidates; 272 have one or two. These are candidate counts before category selection and deduplication, not final card counts.

| Example | Eligible | Above trial threshold | Implication |
|---|---:|---:|---|
| AIK Stockholm vs IFK Goteborg | 8 | 0 | Keep access to records without asserting that every record is typical. No forced top pair. |
| Panevėžys vs Banga | 2 | 1 | Banga Under 4.5 (28/30 vs league 86%) passes; this is a match-total record, so do not misclassify it as Results or Attack/defence. |
| Wexford vs UCD | 6 | 1 | UCD Scored 1+ (29/35 vs league 74%) passes; single-sided attack evidence is valid. |

Threshold sensitivity: fixtures with at least one prominent candidate = 492 at 75%, 449 at 80%, 374 at 85%, 227 at 90%. This does not establish which threshold is accurate.

Recommendation: retain flexible sections and no filler. When no top findings qualify, provide a neutral introduction and access to all eligible records. When only match-total findings qualify, show those without empty Results/Attack-defence boxes or an orphaned Also heading. These empty-state changes are recommendations, not Figma implementation in this pass.

The current test only measures an above-average rate. It cannot flag unusual below-average rates; failure does not establish typicality. A meaningful-difference requirement and a below-average branch need separate decisions. The model treats the league baseline as fixed, uses rounded saved Beta parameters, and shares teams across fixtures. This is a saved-snapshot layout/selection check, not a calibration or future-outcome validation. Venue-mode populations were not assessed.

Figma review 1196:62044 wording changed to “More often than the league average”. Longer labels wrap within existing pair columns; structural overflow check recorded separately. No production logic changed.
