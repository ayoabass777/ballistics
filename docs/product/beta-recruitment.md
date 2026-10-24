# Invite-only beta recruitment and onboarding plan

Recorded 2026-10-21. Draft operating plan for owner review. No public recruitment or launch has been authorized, and the product has not been deployed.

## Audience and cohort

- First locations: British Columbia, UK and Nigeria (D157). Friends in each location are the first usability cohort. Geography means tester residence; ask separately which covered leagues they follow. The current metadata has UK leagues but no Nigerian domestic league.
- Proposed next cohort: 2–3 people outside the owner's network per location, selected for researching upcoming football matches at least weekly. Include a mix of people who bet and people who do not, plus mobile and desktop users. These counts and criteria are proposals until the owner accepts them.
- Recruit adults only for the initial cohort. Present Ballistics as a free football research beta, with no betting required, no staking or winnings claims, and no payment request.

## Short screener

1. Where do you live: BC, UK or Nigeria? Are you 18 or older?
2. Which football leagues do you follow most? How often do you research a match before kickoff?
3. What tools did you use for a recent match, and what was difficult to judge?
4. Would you use a free private beta for one week and give two short feedback sessions? Which device would you use?

Collect an email address only after selecting a tester and explaining its use for the invitation. Record source channel and consent to contact. Do not issue public invitation codes; each code is bound to one invited email.

## X and Reddit outreach drafts

Post only after a remotely reachable beta and the signup journey pass staging. Do not use a localhost URL. Check each Reddit community's rules and ask moderators when recruitment posts are unclear. Use one genuine post in a relevant community, not repeated cross-posts or unsolicited DMs. On X, post from a real project/founder account and avoid duplicated replies or bulk outreach.

**X draft**

> I’m building Ballistics, a free football research beta that helps you inspect team strength, goal estimates and recent patterns before a match. I’m looking for a few adults in BC, the UK and Nigeria who research matches weekly to try it for a week and tell me where the evidence is useful or confusing. No betting or payment is involved. Reply if you’re interested; I’ll send a short screener before any invite.

**Reddit draft (where moderator rules permit)**

> I’m building a football analytics tool and looking for a few adults who research upcoming matches at least weekly. The private beta is free and shows evidence behind team strength, goal estimates and patterns; it does not place bets. I’m especially interested in people in BC, the UK and Nigeria. I’d ask you to try it for a week and share two short rounds of candid feedback. If this is allowed here and you’re interested, comment and I’ll send a short screener. I’m the builder, and this is user research.

## Onboarding is part of the test

The intended first-run flow is skippable and teaches the product's research purpose, the difference between records/estimates/rankings, league choice, a responsible-play note, and Save. A minimal local implementation was added on 2026-10-21. The built-in browser verified selection and skip using a temporarily reset existing account; a fresh invitation/account journey and mobile viewport remain unverified. The more elaborate Figma onboarding is illustrative and not the live implementation. The first accepted invitation account reached Discover before the local flow existed. D37–D40, D133 and D156 remain the relevant agreed direction; do not treat the old plan/account mock as current beta behavior.

Before external testing, test the local sequence from a fresh invite in desktop and mobile browsers: invitation/account creation → skippable explanation of records, estimates and comparisons plus responsible-play note → optional league selection persisted to the account → Discover with a brief Save tip. Skip must go to Discover without selecting leagues. Returning members should bypass it; Settings offers replay of the explanation. Do not mark the onboarding checkpoint complete until the journey is observed end to end.

During the cohort, record per tester: invited → accepted → first run completed/skipped → league choice saved → first Match opened → first fixture saved → return visit after kickoff → feedback. Keep qualitative notes on confusing numbers, trust, data errors and missing leagues; segment by source and location. Small samples are for finding usability problems, not estimating country-level demand or conversion.
