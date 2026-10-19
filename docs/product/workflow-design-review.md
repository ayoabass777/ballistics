# Workflow design review

Started: 2026-10-07. Status: **proposed, for review**.
Scope: page map and first Discover review. No Figma or frontend edits in this pass.
Existing decisions remain authoritative; suggestions below are not new approvals.
Biggest standouts remains **pending review** (D82).

## Evidence and boundaries

- Read the design brief, decision log, UI data contract, prototype links, implementation status and onboarding stories on 2026-10-07.
- Visually inspected current mobile Discover frame `16:359` in Figma on 2026-10-07: https://www.figma.com/design/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359.
- Other page statuses below come from recorded work, not a fresh visual or interaction audit.
- Earlier onboarding stories and README gap lists contain superseded statements. D45/D47/D48/D50 establish paid access with a free preview, password-based authentication, and existing desktop Saved/Account designs. Treat these decisions as the baseline.
- D79/D80 establish a fixed-date standout preview and fixture routes; they do not establish working date/league filtering. D82 now holds that design pending review.

## Page map

| Page / surface | User's task | Entry | Next action / destination | Recorded status and review need |
| --- | --- | --- | --- | --- |
| Welcome | Understand the product | First visit | Preview, get started, sign in | Figma exists; review concise value and access explanation |
| Product tour | Understand the member workflow without fixture data | Welcome · See how it works | Get started or sign in | D131 Figma review; no signed-out standout fixtures or match evidence |
| Onboarding and plans | Learn meanings, choose leagues, create account and get access | Get started | Discover | Figma exists; pricing and purchase behaviour unresolved; password flow supersedes OTP sign-in |
| Discover | Find a fixture worth researching | Primary navigation / onboarding | Choose category; open match | Figma exists; fresh mobile screenshot inspected; hierarchy and scope review first |
| Standout full list | Explore qualifying cross-category fixtures | Discover View all | Open fixture; Back to Discover | Figma only; design pending review, static snapshot |
| Matches | Find a specific fixture or browse the schedule | Primary navigation | Date / team / league filtering; open match | Figma exists; combined filters and return state need review |
| Match details | Understand the evidence for both teams | Discover, Matches, Saved, preview | Save; inspect evidence; open standings or methodology | Figma exists in multiple variants; reconcile full details and newer standout overviews |
| Standings | See league position in match context | Match details | Back to originating match | Figma exists; group and unavailable states matter |
| Head-to-head disclosure | Inspect previous meetings | Match details | Expand / collapse within match | Inline, not a separate page under current direction |
| Saved | Return to a shortlist before and after kickoff | Primary navigation / save confirmation | Open match; compare saved and latest information; remove | Figma exists; review upcoming, past, changed and empty states |
| Forecast record | Inspect recorded forecasts and outcomes | Primary navigation | Filter fixtures; inspect event and correction history | Figma plus local frontend/API work; no deployment claimed; Saved is not a personal accuracy score |
| How it works | Understand what numbers mean | Discover footer / contextual links | Topic detail; return to origin | Figma exists; preserve distinctions between record, estimate and ranking |
| Account and Your leagues | Manage preferences and access | Header avatar / league Edit | Follow/remove leagues, timezone, subscription, password | Figma exists; audit returns to research context |
| Sign-in and recovery | Regain access | Welcome / account flow | Discover | D133 email/password Figma flow with email reset link; Discover destination agreed 2026-10-18 (D139), route verification pending |

## Research journey and return behaviour — proposal

1. Enter Discover with a clearly stated date or date range and league scope.
2. Choose Team strength, Goals outlook or Team patterns; display one list at a time.
3. Open a fixture. Keep the selected finding visible as the reason for entry, alongside the common match-detail structure.
4. Inspect team context, goal estimates and historical patterns as distinct evidence. Open standings or expand head-to-head when useful.
5. Save the fixture with immediate confirmation and a route to Saved; saving need not interrupt research.
6. Back restores the origin's date/range, league scope, category, filters and scroll position.
7. On a return visit, Saved separates upcoming and past fixtures and distinguishes the saved snapshot from later estimates/results.

Shared bottom navigation stays Discover / Matches / Record / Saved (D46); Account stays in the header (D49). Desktop retains the same destinations in its sidebar (D50).

## Discover: first review

Purpose: help a member select a fixture to investigate, while making the basis of selection understandable.

### What is visible now

The inspected 390 × 844 mobile frame shows the branded header, a two-line heading, supporting copy, Saved preview badge, league controls, prototype notice and a horizontal Biggest standouts strip. The strip and View all occupy the rest of the viewport above the bottom navigation. The three research categories are not visible in the initial viewport.

The visible standout section names Sat 10 Oct and UTC. The league control shows Your leagues · 3, but the documented standout snapshot does not recompute for this scope. This creates an unresolved relationship between the controls and displayed cards. Sample labels correctly disclose that the screen is not live.

### Proposed hierarchy for the next mobile review

| Order | Content | Intent |
| --- | --- | --- |
| 1 | Existing compact brand header and Account avatar | Keep orientation and account access |
| 2 | Discover heading with shorter supporting copy | Give space back to research controls |
| 3 | Explicit date/range and Your leagues / All leagues, with Edit | Make the population being browsed clear before showing results |
| 4 | Team strength / Goals outlook / Team patterns | Make all three research routes immediately discoverable; show one active list |
| 5 | Active category description/count and first fixture card | Start useful evidence within the initial viewport where feasible |
| 6 | Remaining ranked cards and How it works access | Continue research and explain the numbers |
| Persistent | Existing bottom navigation | Switch between primary tasks |

Standouts are deliberately unresolved in this proposed hierarchy. Preserve the existing version for comparison; assess placement and visibility separately rather than treating D79 as approval. Compare whether the section helps people choose a match or delays access to their intended category. Do not remove existing frames.

### Card and interaction requirements

- Retain visible team names and crests/initials, fixture order, competition and kickoff.
- Lead with one clear finding; distinguish estimated chance from observed hit/total and strength classification. Do not label ranking scores as probabilities.
- View match is the main action. A Save control may be evaluated as a secondary action without replacing the research path; preserve fixture-level saving.
- Category changes retain date and league scope. Returning from a match restores list position.
- Edit leagues returns to the same research context; the resulting list makes any changed scope clear.
- Preview data needs an explicit fixed date and sample provenance; controls must not imply that a fixed snapshot has been recomputed.
- Kickoff display should follow the agreed browser timezone with account override; legacy UTC prototypes are not evidence that this has been implemented.

### States to design alongside the populated page

| State | Required behaviour |
| --- | --- |
| No qualifying items in this category | Explain that matches may still exist; link to Matches with the same scope |
| No items in followed leagues | Offer All leagues without silently changing scope |
| Loading or refresh | Retain scope and layout; do not flash zero counts |
| Failed request | Keep selected controls; offer Retry; identify retained stale data if shown |
| Missing individual metric | Mark it unavailable; never substitute zero |
| Saved / save failed | Confirm the successful action or give a retry without losing the match |

### Decisions for the next design pass

1. **Date scope:** should Discover default to one matchday or an upcoming range? Proposed starting point: one matchday, with explicit date navigation. This is not agreed and differs from older multi-date examples.
2. **First-screen hierarchy:** review the compact heading → scope → category → first-card arrangement against the existing screen.
3. **Standouts:** remains pending review; decide placement only after comparing its contribution to the core research task.
4. **Duplicate fixtures:** confirm one card per fixture within each category and whether the same fixture may appear in different categories; the decision log contains reopened choices. Do not silently change ranking or deduplication rules.

## Next deliverable and review criteria

Create a separate mobile Discover variant in the existing Figma file using existing tokens/components and saved data with explicit provenance. Preserve current screens. Include a populated category, category-empty state and return-from-detail path. Adapt to desktop after reviewing the mobile hierarchy.

Review success: users can identify scope and all three categories immediately, understand why a fixture appears, reach its evidence, and return without losing their place. Standout placement, date scope and fixture duplication remain explicit review decisions.

## First prototype delivered — 2026-10-07

D83: [Open mobile Discover v1](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=576-54479&starting-point-node-id=576%3A54479). Three category states are built and screenshot-checked in Figma, with existing detail/list destinations reused. This advances the initial document-only pass. Date and league scope stay fixed in this prototype; sample cards are drawn from the existing standout snapshot. Empty states, functional filters, desktop adaptation and browser interaction verification remain future work. Standouts and the combined hierarchy remain pending review.
