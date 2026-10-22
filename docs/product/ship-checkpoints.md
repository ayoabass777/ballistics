# Ballistics 2.0 ship checkpoints

Recorded: 2026-10-20. The date ranges are planning estimates, not deployment authorization. Assumption: one developer focused primarily on launch work; dates move when scope or capacity changes.

## Baseline estimate

- Invite-only member beta: 3–5 weeks from 2026-10-20 (approximately 10–24 November 2026). Owner direction: Clerk accounts, free beta access, no card collection, no Stripe checkout and no seven-day trial clock during beta.
- Paid public launch: 8–12 weeks from 2026-10-20 (approximately 15 December 2026–12 January 2027).
- Forecast record publication, Figma-only Evidence depth (D146), forecast retention activation and other proposals are not assumed to be launch requirements. Their scope must be explicitly decided before including them in an estimate.

The current local product has working Discover, Matches, Match details, Saved and followed leagues, with selected signed-in browser checks. It has not been deployed. The billing schema is tested on a disposable database but has not been applied to the account database; Stripe checkout and webhooks are unbuilt. A seven-day free trial followed by $9.99 or $19.99 per month is an option for the paid product, not a final price or an implemented flow. Decide and document those terms at the end of product development, before paid launch implementation is finalized. See [implementation status](implementation-status.md) and [decisions](decisions.md).

## Checkpoints that trigger a new estimated time to ship (ETS)

| Checkpoint | Reached when | Current state on 2026-10-20 | Recalculate |
| --- | --- | --- | --- |
| 1. Beta scope frozen | Owner decides the beta audience, launch geography and included pages. Open Figma proposals are either included with explicit work or deferred. Beta access is free to invited Clerk users, without a card or trial clock. | Access rule decided; remaining beta scope open. D131 limits signed-out exposure. | Rebuild the beta task list and date range. Keep the paid range provisional until checkpoint 4. |
| 2. Member journey complete locally | Fresh user can sign up with the intended Clerk methods, sign in, choose leagues, reach Discover, open a Match, Save and return to it, see updated/finished status, and sign out. Signed-out requests cannot read member data. Desktop and mobile browser checks pass on real local services. | Core signed-in paths have partial live browser verification; email-only configuration, first-run flow and full fresh-account check remain open. | Reestimate beta from observed defects and remaining deployment work. |
| 3. Deployed beta staging smoke test | Frontend, account API, research API, database, Clerk, scheduled data jobs, secrets and storage run in a production-like environment. Fresh-account, core browse/save, data freshness, failure/alert and rollback checks pass. | Local only; no staging deployment verified. | Replace local-development assumptions with measured deployment and operations work for beta. |
| 4. Product development and beta review | A small external cohort completes the intended journey. Blocking defects are fixed; data freshness and support/incident handling work over at least several daily update cycles. At the end of product development, owner decides paid terms: whether to offer seven days free, $9.99 versus $19.99 monthly, currency, card requirement, and how existing beta users transition. | Not started; pricing and trial remain options. | Reestimate public launch from observed use, support issues and the chosen billing scope. |
| 5. Billing and access complete locally (paid path only) | After paid terms are decided: Stripe test Checkout, signed webhook, idempotent subscription sync, cancellation/expiry, Billing Portal, and backend entitlement checks all pass end to end. Frontend handles denied access. Billing schema is applied through a verified migration. | Schema only; checkout, webhook and enforcement remain pending. Deferred until paid terms are set. | Reestimate paid launch from real integration and failure cases. This checkpoint does not gate beta. |
| 6. Paid launch readiness | Live-mode billing and entitlement checks pass, pricing and legal/customer copy are approved, monitoring and recovery are exercised, and a final release checklist has no blocking items. | Not started. | Set the actual launch date/window; do not infer readiness from a passing local test alone. |

## ETS update rule

When a checkpoint is reached, record its date, evidence links, unresolved blockers and actual time spent. Recount the remaining critical-path work, estimate it using the demonstrated pace, add time for end-to-end QA and defect repair, then publish revised beta and paid-launch **ranges** with assumptions. Move the range at once if a blocker or scope change makes the current estimate untenable; do not wait for the next checkpoint. A checkpoint is complete only with the evidence in its “Reached when” column, not because its target date arrived.

| Review date | Trigger | Evidence | Beta ETS | Paid ETS | Assumptions / blockers |
| --- | --- | --- | --- | --- | --- |
| 2026-10-20 | Initial baseline and beta access decision | [Implementation status](implementation-status.md); [D143 and other decisions](decisions.md) | 10–24 Nov 2026 | 15 Dec 2026–12 Jan 2027 | One focused developer; beta free without card/billing; paid pricing and trial decision deferred to end of product development; no deployment yet. |
| 2026-10-21 | Initial geography narrowed | [D148](decisions.md); [implementation status](implementation-status.md) | 10–24 Nov 2026, provisional | 15 Dec 2026–12 Jan 2027, provisional | Friends in BC, UK and Nigeria are the first cohort; additional tester criteria, counts and included pages are not frozen, so checkpoint 1 has not been reached. |
