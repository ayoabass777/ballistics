# Functionality tutorials for members

2026-10-21 — Draft content plan. On 2026-10-22 the owner asked to implement and review it. The current delivery is local frontend code, with verification limits below; no Figma flow or deployment is claimed.

## Implementation review — 2026-10-22

Implemented locally: account-scoped dismissible tips for Discover, Match details, Saved, Settings and Forecast record; a Matches filter tip; date/league help on Discover; and “Replay feature tips” in Settings. The onboarding Save tip remains the first help shown to a newly onboarded member, so the Discover tip waits until another visit. The Forecast record tip appears only after record data is available. Each tip is short and skippable; Settings clears dismissal state for the signed-in account and returns to Discover. Focused research and Forecast TypeScript checks passed. Browser interaction and visual checks remain pending.

The after-kickoff lesson remains pending until the last pre-kickoff forecast display is implemented and verified. Forecast record publication is deferred past the beta under D152; its tip is present in the local page but is not counted as beta scope. These tutorial choices do not change the underlying forecast, filtering, Saved or preference contracts.

## Match details first-visit walkthrough — raw ideation, 2026-10-22

The first complete copy and interaction draft is [First Match details tutorial](match-details-tutorial-draft.md). It includes the recently added Favourite and Slight edge states. The owner authorised local implementation later on 2026-10-22; focused TypeScript passed, while browser interaction and visual checks remain pending.

The owner suggested a quick first-visit tutorial centred on the three analysis sections. This is a content/design proposal, not a request to replace the current generic Match details tip or an implemented three-step walkthrough. Keep it short, skippable and replayable; show each explanation beside its actual section, rather than teaching all three in one introductory paragraph.

1. **Team strength — who has the edge?** Explain Big dog/Underdog as a relative strength label supported by historical league results and team ratings. A Big dog label is shown only when the league and fixture qualify. Other matches can show Favourite, Slight edge, Closely matched or no Big dog label. Suggested plain-language draft: “See whether one team has a clear strength edge. A Big dog is a stronger side in a league where past results support that label; the other side is the Underdog. It describes expectation, not a guaranteed result.” Avoid equating the label directly with club reputation or table position.
2. **Goals outlook — what goal outcomes are estimated?** Explain that this section shows specific goal-market outcomes and their estimated probabilities, using the model's scoring evidence. Suggested draft: “See which goal outcomes the model estimates for this match. Each percentage is a chance for the named outcome, not a promised score or exact goal range.” Do not promise a continuous goal range unless the UI actually displays one. Do not describe the displayed percentage as a simple average of the two teams' observed attack/defence rates.
3. **Team patterns — what has happened this season?** Explain current-season hit/total records for results, scoring, conceding and match goals, with venue scope and comparison to the same event/scope league baseline where available. Suggested draft: “See the results and goal patterns each team has repeated this season. Compare each record with the league's usual rate for that same event and setting. These are past results, not forecasts for this match.” Avoid reducing the section to generic “form” or claiming every row is goals per game.

Implemented interaction: one callout beside each available section in Strength → Goals → Patterns order, with Back, Next, Skip and Done. Absent sections are skipped. Existing account-scoped dismissal and Settings replay are reused. Review actual match payloads and mobile layout before marking the experience browser-verified.

## Purpose

The first-run introduction explains what Ballistics is and how to read its numbers. Members also need help with the actions they can take once they reach the product. Show each lesson where the action happens, keep it dismissible, and offer a replay path through Settings or How it works. A member should be able to use every page without completing a tutorial.

## Proposed lessons

| Where | Member question | Tutorial content | Trigger |
| --- | --- | --- | --- |
| Discover | “What am I looking at?” | Strength, Goals and Patterns are different ways to find matches worth researching. A card is a starting point; open it for the supporting evidence. | First visit to Discover, after the first-run introduction. |
| Discover / Matches | “How do I narrow this down?” | Choose a date and leagues. “Your leagues” uses leagues saved in Settings; “All leagues” broadens the view. Matches is a chronological list of fixtures in the current research snapshot, not every scheduled match. | First use of a filter or an unobtrusive help control. |
| Match details | “How do I read this?” | Context is observed team information; Goals is an estimate; Strength is a relative label; Patterns describe past results. These are evidence to assess, not a result or betting instruction. Link to How it works for definitions and limitations. | First opened match. |
| Match details | “What happens after kickoff?” | Explain that an available pre-kickoff forecast remains labelled as such after kickoff; the match status and result may change. | First post-kickoff detail view, once this behavior is implemented and verified. |
| Save / Saved | “What does Save keep?” | Save keeps the fixture and what its card showed at that time. Saved lets members revisit it, see its current match status and notice a comparable update; removing it clears it from their list. | First Save, then a small empty-state explanation in Saved. |
| Settings | “How do I personalize this?” | Follow leagues to use “Your leagues” on Discover and Matches; adjust the displayed timezone or return to automatic. Offer replay of the introduction and these lessons. | Help control near the relevant setting. |
| Forecast record | “Has the model been right?” | Explain the scope of the published record, settled versus pending entries and where methodology is documented. Avoid implying a personal betting record or a guarantee. | First visit to Forecast record, after checking its current data and availability. |

## Delivery proposal

Start with brief, one-step tips for Discover, Match details and Save/Saved. Put fuller explanations in How it works and link to them from the tips. Use plain language and one action per tip. Do not interrupt filtering, browsing, or return visits with a sequence of modals. Record dismissal per account if implemented; replay must remain available.

Before implementation, confirm final copy, placement, replay behavior and which tips belong in the beta. Verify each lesson against the running UI and current data contract. The existing first-run Save tip may satisfy part of the Save lesson; avoid showing it twice.
