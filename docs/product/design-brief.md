# Design brief

Recorded: 2026-09-29.

## Product direction

Ballistics 2.0 is a football research experience that makes a dense collection of fixture data understandable. The user can model football outcomes but does not want to imply that forecasts are perfect. Transparency is a core product principle, not a disclaimer added at the end.

Potential audiences discussed include Nigeria, East Africa and parts of Europe. These are exploration ideas, not validated market choices. The owner does not yet have a football community. Pricing, monetisation and launch geography remain open.

## Experience

- **Match context:** compare both teams side by side for goals scored/match, conceded/match, attack rank and defence rank. Prototype defaults to home team's home split and away team's away split, with an Overall toggle. Highlighted patterns remain separate.
- **Discover:** a ranked starting point for research, separate from the complete fixture browser.
- **Matches:** chronological fixture browsing, with deeper analysis per fixture.
- **Discover categories:** Strength advantages, Goals outlook, Consistent patterns. Show one category/list at a time, not all sections expanded together.
- **Main interface:** football language, no model picker and no model names such as Elo or Bayesian. Technical provenance may be explained in Methodology.
- **Patterns:** scoring, conceding, wins, losses, draws, unbeaten/winless and other supported events; not just unbeaten runs.
- **Pattern cards:** emphasise event, team/venue context and season hit/total. Keep league averages and consecutive runs in fixture details.
- **Goal forecasts:** use Dixon–Coles as the product's forecast source. Historical goal-related patterns remain evidence, not a competing goal forecast.
- **Saved, Forecast record, Methodology:** retain these product capabilities; omission from the latest flow was temporary, not a removal decision.

## Transparency

Separate observed records, adjusted estimates and future forecasts. Show sample size and context; missing values must not become zeros. Label static exports/mock data and their dates. Do not imply independent confirmation from related signals. The intended forecast record should include misses as well as successes.

## Visual baseline

Existing Figma direction: Inter typography, deep green brand/navigation, off-white canvas, white evidence panels and pale-green emphasis. Desktop and mobile layouts share the same hierarchy. Reuse existing components and tokens before creating a new design language.

## Open questions

- Final navigation placement for Saved, Forecast record and Methodology.
- Production ranking rules across different pattern events, beyond the current selected-example ordering.
- Scope of filters and progressive disclosure within the full fixture-analysis view.
- ~~Timezone presentation~~ — resolved 2026-10-02 (D41): detected from browser, overridable in settings.
- Persistence/authentication for Saved and the evaluation rules behind Forecast record.
