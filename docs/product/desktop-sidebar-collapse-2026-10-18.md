# Desktop sidebar collapse — handoff

Updated 2026-10-19. Decision D135 is approved for the compact desktop Discover look. The owner reviewed [the Strength prototype](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-354&starting-point-node-id=334%3A50895) and said, “I like this new look. Document it so we can continue later.” Keep the current icon rail and toggle treatment as the reference when continuing desktop navigation work.

## Agreed behavior

- Desktop starts with a 224px labelled sidebar. Collapse switches to a 72px icon rail; expand restores labels. Navigation order and destinations remain Discover, Matches, Forecast record, Saved.
- The local frontend remembers the collapse choice in browser storage. Mobile keeps its bottom navigation.
- This approval concerns the new sidebar look and behavior. It does not approve unrelated account, pricing or navigation changes.

## Current delivery and evidence

- On 2026-10-19, the shared Figma collapse pattern was extended across 301 additional four-link desktop sidebars on the active Discover page: Matches, Snapshot and fixture details, Saved, Account and league settings, plus 1440/1280/1024 review variants. The current desktop Forecast record uses a separate five-link `Navigation` structure; all 66 variants now use the same 224px/72px variable-driven rail, with a compact `?` entry retaining the existing How it works route. The shared variable default was restored to expanded after visual review.
- Compact screenshots were inspected for Matches, Saved and Forecast record; expanded Forecast record was also inspected on 2026-10-19. Existing link reactions were copied to compact counterparts structurally. Full click-through regression across these added routes remains open.

- Figma expanded and compact states are wired on desktop Discover Strength `16:354`, Goals `22:452`, and Patterns `22:547`. The shared Figma variable collection `VariableCollectionId:1769:192` controls sidebar width, padding and which set of links is visible. The design default is expanded. Existing destinations were copied to the compact links.
- The Goals prototype was clicked through Collapse → Expand → Collapse on 2026-10-18. Expanded and compact screenshots were inspected. [Compact screenshot](mock/desktop-sidebar-collapsed-2026-10-18.jpg).
- Local site implementation is in the original project's `frontend_dev/components/research/shared.tsx` and `research.module.css`. The shared Shell makes the control available on desktop routes. Scoped `npm run typecheck:research` passed on 2026-10-18.
- The temporary Next.js dev process did not open its test port during this pass, so live site visual interaction is unverified. This work is local; no deployment is claimed.

## Continue later

The owner flagged on 2026-10-19 that Matches `16:355` had legacy main content despite the new sidebar. The active content was replaced on 2026-10-19 after Figma access resumed; the old content remains hidden. The current code layout is defined by `frontend_dev/components/research/MatchesControls.tsx` and `DiscoverPage.tsx` in the original project. The full browser click-through and older linked day-variant review are still open.

### Matches correction and remaining limits

- Kept the brand header and D135 sidebar. The old Matches by day content is hidden as a historical reference.
- The active Figma hierarchy now has **Matches**; “Browse upcoming matches by date and league”; Next 7 days / Today / Tomorrow / Date controls; date scope; All leagues / Choose leagues plus signed-in Your leagues; illustrative snapshot disclosure; fixture count; three-column compact fixture grid; and How it works. The current code adds Show more matches when more than 12 results exist; the Figma sample contains six, so it has no expansion button.
- Each Figma fixture card has paired club crests, competition logo/name/country, kickoff, View match and Save fixture. The six 10 Oct fixtures are explicitly illustrative; they are not a current engine result.
- Date tabs switch existing sample/empty states. Choose leagues opens an editable search-and-country overlay, and Your leagues switches this illustrative sample to three fixtures. Search entry and arbitrary checkbox filtering are visual examples in Figma; the live behavior is in code. Review the older linked day variants and click through representative detail, Save and return routes before treating the entire prototype journey as current.

1. Verify the local site's expand/collapse interaction and persistence at desktop widths, including a route change and a reload. Check the 700px mobile breakpoint retains the bottom navigation.
2. Check icon navigation destinations and active state by clicking representative added screens, including Forecast record and the 1280/1024 review variants. Preserve historical frames and prior prototype designs.
3. Remaining legacy two-link details, league-picker review frames, older prototype page and focused All standouts view were not converted in this rollout. Reassess these only if they become part of the current desktop journey.
