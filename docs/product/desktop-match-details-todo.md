# Desktop Match details — to-do

Recorded 2026-10-12 at the owner's request. Status: first Figma review built following owner authorization to start. Production implementation and remaining checks are pending.

## Proposed layout

- Reuse the existing desktop navigation and workspace shell.
- Place Match context across the top.
- Below Context, place Team patterns in the wider left column.
- Stack Team strength and Goals outlook in the narrower right column.
- Expand patterns inline within their column, without nested scrolling or pushing Goals down. Check the resulting column balance.
- Keep one concluding limitations block.

This supersedes the earlier suggested arrangement of Context/Strength left and Patterns/Goals right as the next layout to explore. Preserve existing desktop designs for comparison.

## Content and behaviour to carry over

- Use the current mobile review as the content reference: [1196:62044](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044).
- Keep Top Results and Top Attack/defence categories, without duplicate records in Also or expansion.
- Use existing Bayes display eligibility; compare historical records with the league baseline for prominence. Do not reinstate the rejected extra 80% probability-above-league test. Precise distance/ranking boundaries remain unresolved in the contract.
- Retain independent Home / Away and All venues pattern scope, with matching records and league baselines and honest unavailable states.
- Retain See all patterns and Show fewer at both ends; bottom collapse returns to Patterns. Derive the additional count from the actual list; do not pad Also.
- Keep the lime team-name accent and league-history wording: “Big dogs usually win in this league” / “Based on results across several seasons.” Do not restore the numerical league count or Reliable here label.
- Keep explicit “More/Less often than the league average” comparisons and the supplied historical counts.
- Preserve fixture identity, source dates, Save/navigation behaviour and any missing-data distinctions. Prototype delivery does not imply production functionality.

## Checks before completion

- [x] Build a separate desktop review using existing design styles and the current mobile content.
- [x] Review collapsed and expanded patterns, including isolated synthetic sparse and long-list cases.
- [x] Check column balance, text wrapping and overflow at 1440, 1280 and 1024px review widths.
- [x] Check long team names in Patterns and single-sided/empty category states.
- [x] Check long names in the narrow match header and Strength card using isolated synthetic copies.
- [x] Verify both independent scope switches, expand/collapse and return-to-Patterns in the desktop prototype.
- [ ] Complete external navigation regression; inherited routes still include legacy screens.
- [x] Remove duplicate limitations blocks from the new review.
- [x] Capture visual evidence and record prototype links and verification status.

## Inspection evidence

Figma structure inspected 2026-10-12:

- Older full desktop detail `348:52917`: 1440px wide. Context and Strength precede side-by-side evidence panels. Patterns `348:52967` is 568×932; Goals `348:53017` is 568×293. Two concluding blocks `348:53035` and `348:53038` are both visible. These dimensions suggest substantial column imbalance; new layout needs visual verification.
- Saved standout detail `542:54175`: 760px-wide vertically stacked preview using older Radnicki/Crvena data, not the current mobile design.

2026-10-12 — First [desktop Match details review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1226-62085&starting-point-node-id=1226%3A62085) built at 1226:62085 using current mobile cards and existing desktop shell. Context across top; Patterns 680px left; Strength/Goals stacked right. Nine qualified patterns preserved, one limitations block. Browser verified expansion, bottom collapse returning to Patterns, and Home/Away unavailable state; Goals remains in place during expansion. Screenshot checked at 1440px. Narrower layouts, long-name/list stress cases and exhaustive external navigation remain pending. Inherited sidebar and external detail links retain existing prototype destinations, including older/mobile flows. Figma-only, no frontend implementation.

2026-10-12 — Desktop follow-up QA completed for review variants: 1440px `1226:62085`, 1280px `1235:62127`, 1024px `1235:62521`. Narrow view stacks Context subcolumns and evidence sections; 1280/1440 retain Patterns left and Strength/Goals right. Fixed Context scope controls changing width between states, misleading fixed bold styling on the All venues pattern label, and excessive fixed-height gaps in the 1024px layout. Added wrapping for Context team names and the attack/defence pair label. Browser clicked both Context scopes and both Pattern scopes in both directions; confirmed their state independence, venue unavailable message, restoration of all-venues records, See all, and bottom Show fewer returning to Patterns. Separate synthetic QA cards cover long names with twelve expanded rows (`1240:62211`), one-sided Results without Also (`1240:62397`), and empty categories (`1240:62538`); visually checked at the narrower 560px pattern-card width. Synthetic QA records are layout fixtures, not qualified football evidence. Figma-only; these are separate breakpoint frames, not responsive frontend implementation. External sidebar/detail navigation still retains legacy routes and is not fully regression-tested. Additional isolated long-name checks passed for the 736px match header (`1243:62211`) and 408px Strength card (`1243:62241`); Strength wraps to two lines without overlap.

