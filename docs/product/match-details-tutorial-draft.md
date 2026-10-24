# First Match details tutorial — draft

2026-10-22 — Copy and interaction draft. Later on 2026-10-22 the owner asked to apply it to the live local frontend. The three-section guide is implemented locally and passes the focused research TypeScript check. A signed-in local browser opened OFK Beograd–FK Crvena Zvezda and verified all three guide steps, Back navigation and desktop callout placement. Mobile layout, absent-section examples and Figma remain unverified; no deployment is claimed.

## Entry

Show on the first opened Match details page, after the page data loads. Keep the match visible. The guide points to one section at a time and offers **Next**, **Back**, **Skip guide** and, on the final step, **Done**. If a section has no data, skip that step. A Settings replay entry can reopen the guide. Do not block the page or repeat the guide automatically after dismissal.

Opening line: **“Three ways to understand this match”**

Supporting line: “Team strength, Goals outlook and Team patterns answer different questions. Take a quick look at each.”

## Step 1 of 3 — Team strength

**Title:** “Who has the strength edge?”

**Core copy:** “This compares the teams’ strength using ratings and past league results. It shows an expectation for the matchup, not a certain winner.”

Add one short line that matches the state actually on screen:

| Displayed state | Tutorial line |
| --- | --- |
| **Big dog / Underdog** | “A Big dog has a clear ratings edge in a league where similar favourites have a strong past record. The other team is the Underdog.” |
| **Favourite / Underdog** | “There is a clear strength edge, but this match does not meet the extra league-history rule for a Big dog label.” |
| **Slight favourite / Slight underdog** | “One team has an edge, but the gap is smaller.” |
| **Slight edge / Slight underdog** | “One team has a small edge. Treat it as context, not a strong verdict.” |
| **Closely matched** | “The teams are close on this strength measure.” |
| **No Big dog label** | “There is not enough qualifying evidence for a Big dog label here. That alone does not mean the teams are equal.” |

If Top/Middle/Bottom tier rows are present: “These tiers group team ratings within this league; they are not league-table positions.” This is optional inline help, not an extra tutorial step.

**Next:** “Look at goals”

## Step 2 of 3 — Goals outlook

**Title:** “What goal outcomes look likely?”

**Copy:** “Each row shows a goal outcome and the model’s estimated chance of it happening in this match. The estimate draws on scoring and conceding evidence, but it is not a promised score or an exact goal range.”

**Small note:** “Related goal outcomes can overlap, so several high percentages are not separate confirmations.”

**Optional link:** “How the model works” → the Match details methodology disclosure, now expanded to name the Dixon–Coles based goal model and state its limits.

If the section says **Nothing stands out**, explain: “The available estimates do not identify a standout goal outcome here.”

**Next:** “Look at patterns”

## Step 3 of 3 — Team patterns

**Title:** “What has each team been doing this season?”

**Copy:** “These are observed results and goal patterns from this season. A count such as 6/8 means that event happened in six of eight relevant matches. The league comparison uses the same event and home/away or all-venue setting.”

**Small note:** “Patterns describe what has happened, not the chance of it happening next time. Smaller samples can change quickly.”

If no qualifying patterns appear, explain: “There is not enough qualifying pattern evidence to highlight in this view.”

**Final action:** “Done” · Secondary link: “Read how the numbers work” → the existing Match details methodology disclosure.

## Copy checks before implementation

- Match the exact strength state returned for the fixture; do not always teach Big dog when the UI shows Favourite or another state.
- Keep “rating edge” distinct from club reputation, league-table rank and a match result prediction. The Big dog label has an additional league-history gate.
- Describe Goals as named outcome probabilities. Do not claim the UI supplies a continuous range of expected goals or that the displayed percentages are raw attack/defence averages.
- Keep Team patterns’ season counts distinct from Goals probabilities. “Form” is useful introductory language, but records can include results, scoring, conceding, clean sheets and match totals, with different venue scopes.
- Review desktop/mobile placement and screen-reader focus order with real match payloads, including absent-section and no-label examples.
