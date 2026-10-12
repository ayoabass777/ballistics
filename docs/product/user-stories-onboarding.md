# User stories, Saved workflow and onboarding

2026-10-11 — D109 update: Pick your leagues now supports six independent toggle rows in the review prototype; selected states use a checkmark and tinted surface. Use All leagues clears those prototype selections. This supersedes the fixed Super Liga demonstration described below. Selection variables are local to this review; downstream multi-league filtering and account persistence remain unimplemented.

2026-10-11 — D108 (prototype for owner review): Refreshed onboarding in a separate mobile review flow. Welcome teaches Discover → evidence → Saved; the explainer distinguishes observed patterns, goal estimates and relative strength using the current frozen sample. Four setup steps retained. League screen is a fixed Super Liga demo, followed by phone/email account mock, unpriced plan mock and honest completion copy. Dismissible Discover and first-fixture detail tips added. Originals preserved; no account/payment/persistence functionality or desktop refresh. Pending design approval.

Started 2026-10-02. Status: draft. **Agreed 2026-10-02: D37 (Saved = fixtures + followed leagues), D38 (account from day one), D39 (leagues filter Discover and Matches), D40 (acknowledge betting + responsible-play note), D41 (timezone from browser), D42 (phone or email sign-in), D43 (Strength = engine shortlist).** Everything else here is still proposed.

## Who it's for (working assumption)

Primary: a **football-literate researcher** who looks at a matchday's fixtures before kickoff. Many will also bet. The product doesn't tip, but it should be honest that this is the likely context (the existing "Before you draw a conclusion" copy already mentions bookmaker value). Audiences (Nigeria, East Africa, parts of Europe) remain unvalidated (design brief).

What they want:
- "Show me what's worth a look today without reading 600 fixtures." → Discover
- "Let me check a specific match or league." → Matches
- "Convince me with evidence, not a verdict." → Match details
- "Let me keep my shortlist for this matchday." → **Saved**
- "Has this thing actually been right before?" → Forecast record, Methodology

## Core loop

```
Discover / Matches ──► Match details ──► Save ──► (kickoff) ──► Saved shows result
       ▲                                                              │
       └──────── Forecast record / Methodology ◄── "can I trust it?" ─┘
```

The loop closes on return visits: the user comes back after matches, sees what happened to their saved fixtures, and checks the Forecast record. That return visit is the retention mechanism, so Saved matters more than its current "pending" status suggests.

## User stories

### Discover and research (existing design, written down for completeness)
- US1. As a researcher, I see one ranked list at a time (Strength, Goals, Patterns) so I'm not overwhelmed.
- US2. As a researcher, I open a fixture and see both teams' context, patterns and the goal estimate, clearly separated as record, estimate and ranking.
- US3. As a sceptic, I can reach How it works and the Forecast record from anywhere I see a number.

### Saved — options to choose between

| Option | What gets saved | Pros | Risks |
| --- | --- | --- | --- |
| **A. Matchday shortlist** (recommended for v1) | A fixture, plus a snapshot of what was shown when saved (labels, estimate, capture time) | Simple; matches how people research a matchday; snapshot keeps it honest if numbers change later | Needs an outcome view after kickoff |
| B. Save a specific estimate | One event, e.g. "Brooklyn v Detroit · Under 3.5 · 81.7%" | Personal record mirrors Forecast record | Becomes a bet slip; a personal hit rate invites accuracy claims the product avoids |
| C. Follow teams/leagues | Preferences, not fixtures | Personalises Discover/Matches; pairs with onboarding | Not a shortlist; doesn't close the loop |

Draft stories if A (+ C via onboarding):
- US4. As a researcher, I tap Save on a fixture card or in match details and get immediate confirmation.
- US5. As a researcher, I open Saved and see upcoming saved fixtures grouped by day, in my timezone.
- US6. After kickoff, a saved fixture shows the final score, and for the saved goal estimate, whether it happened. It says Pending, Void or Unavailable where relevant, the same statuses as the Forecast record.
- US7. If an estimate changed after I saved it, I see both "when you saved" and "latest", never a silent overwrite (same principle as the Forecast record's correction history).
- US8. Settled fixtures move to a Past section; I can remove any saved item.
- US9. With nothing saved, the empty state explains what saving does and links to Discover.

Persistence (agreed, D38): **account from day one.** Saved fixtures and followed leagues sync across devices. Sign-in (D48, supersedes D42): phone number or email plus password; reset by email link or SMS code. Open: signed-out browsing limits, data retention, privacy copy, SMS cost/provider for phone codes.

- ~~US10. Browse before sign-up~~ — superseded by D45 (paid, no free browsing).
- US10b. As a new user, I complete onboarding, verify my phone or email, and choose a plan before seeing Discover.
- US10c. As a curious visitor, I can see today's top 3 Strength fixtures for free before deciding to pay (D47).
- US11. As a signed-in user, my Saved list and leagues are the same on phone and desktop.

Placement proposal: Saved in primary navigation next to Discover, Matches and Forecast record. Mobile then has 4 items in the 350px row, so this needs a layout check (icons + short labels, or a bottom tab bar).

## Onboarding (first run)

Goal: in under 30 seconds, set expectations (research, not tips), teach the three kinds of number, and personalise leagues. Skippable at every step; never blocks the content.

1. **Welcome** — one line: "Football research for every fixture — evidence, not tips." Buttons: Get started / Skip. Includes the responsible-play line (D40), e.g. "If you bet, bet only what you can afford to lose." Final wording and any local helpline links are open.
2. **Three kinds of number** — one illustrative card with three tagged parts: *Season record* (what happened), *Estimated chance* (what might happen), *Ranking* (compared with the league). Reuses Methodology topic 1 copy.
3. **Pick your leagues** — reuses the country-first league picker (D29). Filters Discover and Matches (D39); both get a "Your leagues · All leagues" switch. Skip = all leagues.
3b. **Create account** (D38) — sign-up/sign-in, skippable ("Browse first"); Save triggers it later (US10). Leagues picked before sign-up carry over.
4. ~~Timezone step~~ — removed (D41). Timezone comes from the browser automatically; kickoff times show local time with the zone abbreviation; override in settings.
5. **Land on Discover** with one dismissible tip on the first card: "Tap Save to build your matchday shortlist."

Re-entry: How it works → "Replay the intro"; leagues and timezone are editable in a small settings sheet (which also needs an entry point).

First-visit states elsewhere: Saved empty state (US9); Forecast record first visit explains Pending and the synthetic/demo label until the backend is deployed (D35).

## Settled 2026-10-02

Saved model A + C (D37); account from day one (D38); leagues filter Discover and Matches (D39); acknowledge betting with a responsible-play note (D40); timezone from browser (D41); phone or email sign-in (D42).

## Still open

1. Signed-out browsing limits.
2. Saved placement in navigation (4 items on mobile).
3. Responsible-play wording and per-country helplines.

## League management (D49)

- US12. As a member, I tap Edit on Discover's league switch (or Account → Edit leagues), search or browse countries, tap + Follow, and Discover/Matches update.
- US13. As a member, I remove a league with Remove ×; at least one league or All leagues stays available.
- Open: should a match detail offer "+ Follow this league"? (Not built.)

## Desktop prototype (D50)

- Onboarding & paywall: [https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-50895](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-50895&starting-point-node-id=334%3A50895)
- Saved: [https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-52335](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-52335&starting-point-node-id=334%3A52335)
- Account & leagues: [https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-51675](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-51675&starting-point-node-id=334%3A51675)
- Mobile Account & leagues: [https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-49755](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-49755&starting-point-node-id=334%3A49755)

## Prototype built 2026-10-02 (mobile first, for review)

- Onboarding & paywall flow: [start](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=324-47839&starting-point-node-id=324%3A47839). Welcome → Three kinds of number → Pick leagues → Create account (phone 324:47917 / email 324:47964 toggle) → Enter code → Choose plan → You're in → Discover. Welcome's Sign in → Sign in 325:48293 → code 325:48308 → Discover.
- Saved flow: [start](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=325-47970&starting-point-node-id=325%3A47970). Brooklyn detail → Save fixture → Saved state with toast → View Saved → Saved list 325:47880 (upcoming with snapshot and a "changed since you saved" example, a settled result) → View match links. Empty state 325:47942.
- Bottom tab bar approved and rolled out to all live mobile screens (D46).
- Teaser: [Welcome → Preview today's matches](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=324-47839&starting-point-node-id=324%3A47839), screen 329:47955 (D47).
- All screens live in section 324:47838 on page 16:353. Prices are "TBD"; changed estimates and results are synthetic and labelled. Not browser-tested.

## Figma work once agreed

- Saved: populated (upcoming), post-kickoff (hit/miss/pending/void), changed-since-saved, empty; desktop + mobile; Save control on cards and details with confirmation.
- Navigation with 4 primary items on mobile.
- Onboarding: welcome, three kinds of number, leagues, account (phone or email + code) × 2 viewports, plus skip paths and a settings sheet (leagues, timezone, account).
- Save-while-signed-out → sign-up → returns with the fixture saved.
- "Your leagues · All leagues" switch on Discover and Matches.
