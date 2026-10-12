# Discover interaction handoff

Updated 2026-10-07. Scope: current mobile Discover, full standout list, linked saved-sample details, and session Saved shortlist. Figma delivery is not frontend implementation or approval of standout ranking/design.

## Interaction requirements

- Save is fixture-level and idempotent across categories, carousel, full list and details. Saved has one card per fixture; remove clears every placement. Prototype now covers 80 frozen fixtures.
- Save and View match are separate click/tap targets. Hover never saves or navigates. Pointer exit restores the default appearance. No mouse-down/up reaction should compete with click.
- New Save controls and Saved View/remove controls use 44px height. Production controls should have at least 44×44px hit areas; existing compact Discover controls need implementation-level audit.
- Keyboard: semantic buttons/links, logical Tab order, visible 2px green focus outline with 2px offset, Enter/Space activation, Escape discards a league draft and restores focus to Edit. After removing a fixture, focus moves to the next fixture or empty-state action. Announce save/remove and newly revealed count without moving focus unexpectedly.
- Show more reveals remaining cards in place. Keep focus on a meaningful control/status after the trigger disappears. Carousel supports touch swipe plus keyboard-operable previous/next controls in implementation; never require hover to expose an action.
- League picker: draft until Done, Cancel discards; applied count shared across current Discover routes. Single Serbia/Super Liga demo only; Matches picker remains independent. Empty state has no category and following from it defaults to Strength.
- Account persistence, authentication, cross-device syncing, live windows/data, timestamps and snapshot capture remain backend/frontend work.

## Verification boundary

Browser tests cover representative full-list Save → details Saved status → shortlist with one fixture. Earlier passes cover category expansion, carousel, navigation, league Done/Cancel, and multi-section saves. 80 full-list cards and 80 linked detail screens have Save controls; 80 distinct Saved cards share per-fixture state. New Save hover variables are isolated per control. Saved View/remove hover and 44px targets added.

Real touch-device testing, semantic keyboard/screen-reader checks and exhaustive all-fixture click testing are not completed in Figma. These are explicit implementation acceptance checks, not claims of accessible working code. Standout design remains pending owner review under D82.
