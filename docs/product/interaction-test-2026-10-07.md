# Discover interaction test — 2026-10-07

Scope: mobile Figma prototype, current Discover flow beginning at `592:54464`. Tests ran in the in-app browser. These results concern prototype interactions only, not frontend or backend functionality.

## Defects fixed and retested

- **Save click blocked by press reactions.** The hover background appeared but Save did not execute. Removed competing MOUSE_DOWN/MOUSE_UP reactions from all 78 existing Save containers across nine Discover states. Mouse-enter/leave feedback and child click actions remain. Retested both a category card (Rangers) and first standout (FK Crvena Zvezda): Saved confirmation appears and the correct fixture reaches Saved. The pressed appearance remains on the design review board; it is no longer a runtime reaction.
- **Show more jumped to the page top.** Reproduced on Strength despite resetScrollPosition=false. Replaced frame navigation with boolean-controlled visibility on each of the three initial category screens. Added the remaining 4/5/5 cards from existing expanded screens, retaining fixture Save bindings and links. All three categories now reveal cards in place. Old expanded screens remain preserved.

## Browser results

| Scenario | Result |
| --- | --- |
| Strength expands 5 → 9 | Pass; same viewport position, ninth fixture and final count visible |
| Goals expands 5 → 10 | Pass; same viewport position, tenth fixture and final count visible |
| Patterns expands 5 → 10 | Pass; same viewport position, tenth fixture and final count visible |
| Switch Strength → Goals → Patterns | Pass; category-specific cards and selected controls update |
| Save category fixture / remove | Pass; Rangers appeared in Saved and removal returned to empty guidance |
| Save standout and separate Goals fixture | Pass; FK Crvena Zvezda and Amed both retained after category, detail and league navigation |
| Shared fixture status | Pass; the same first standout remains Saved in other categories and Your leagues |
| Carousel swipe / horizontal scroll | Pass; reached fifth standout; free scrolling rather than snapping |
| Explore match / Back | Pass for Rangers; correct details and return to Patterns with fifth carousel position retained |
| All standouts / Back | Pass; frozen 80-item list opens and returns to current category |
| Your leagues from Patterns | Pass; filtered standout plus explicit no-pattern-matches sample state |
| Hover feedback | Save fill observed on pointer entry/click and cleared after moving away; standalone hover-only and exhaustive target/contrast checks remain pending |

## Remaining scope

League-picker Done/Cancel and selection consistency remain the documented bounded-demo limitations (D92), not validated by this pass. Keyboard focus and actual touch-device behaviour remain pending. No exhaustive test of every card or all navigation destinations is claimed. Full-list/detail Save rollout and broader Saved hover remain pending. Account persistence is explicitly deferred to implementation.

Evidence: [Saved with fixtures from two sections](interaction-test-saved-2026-10-07.jpg). Final screenshot taken from the running browser after the fixes. Current prototype: https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464
