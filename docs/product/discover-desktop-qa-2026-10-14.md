# Desktop Discover QA — 14 October 2026

Figma-only. Separate breakpoint frames; no responsive frontend implementation.

## Delivered and checked

- Match details sidebars `1226:62462`, `1235:62135`, `1235:62529` already had simplified styling when inspected: hidden Workspace/helper/divider, borderless inactive items, green active Matches. Earlier pending notes are stale. No redundant mutation made.
- Added 44px Previous/Next controls to three active 1440px Discover carousels and six narrower review copies. Each scrolls to indexed cards with clamped endpoints. Existing horizontal scrolling retained.
- 1280 and 1024 reviews use two-column compact-card grids, filling the available content width. Category navigation remapped within each width; original 1440 screens and checkpoints preserved.
- Figma screenshots inspected for all six narrow category layouts; no compact-card overlap observed in default state.
- Browser at 1024: Next advances first visible card from Radnicki to Riga; Previous restores Radnicki. Compact Save changes to Saved and updates the featured card. View opens matching Radnicki/Crvena desktop detail. Strength Show more reveals 9/9 fixtures. Strength → Goals → Patterns remains within 1024 frames.
- Structural audit: all 87 compact-card placements across 9 category/width combinations retain View and Save reactions (29 per width). This is not 87 individual browser click tests. Goals Save was clicked but its state was not separately captured before switching category.

## Review frame IDs

| Width | Strength | Goals | Patterns |
|---|---|---|---|
|1440|16:354|22:452|22:547|
|1280|1342:63225|1342:64465|1342:65661|
|1024|1342:66881|1342:68121|1342:69317|

## Limits

- External fixture detail destinations retain wider desktop frames and do not adapt to the narrow review viewport. Browser View action confirmed identity, not narrow detail usability. Back within that detail was not confirmed; browser Back returned to Discover.
- Arrow index uses prototype variables, shared between width copies of a category. Manual swipe/trackpad scrolling does not update that index; mixing the two can jump to the indexed card. A production carousel must derive its index from scroll position. No claim of seamless mixed-input behavior.
- All standouts/Saved/account and calendar remain shared existing destinations; no full external-route regression.
- Show more was browser-tested on Strength; Goals/Patterns expansion retains copied reactions but was not separately browser-tested in this pass.
