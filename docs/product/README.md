# Ballistics product memory

Established: 2026-09-29. Owner: project owner. Scope: Ballistics 2.0 design and implementation.

Start here before continuing design or frontend work:

1. [Design brief](design-brief.md): product intent and experience principles.
2. [Decisions](decisions.md): agreed direction, proposals and superseded choices.
3. [Data contract](data-contract.md): UI meanings and current source fields.
4. [Implementation status](implementation-status.md): Figma versus code versus verification.
5. [Prototype links](prototype-links.md): current flows and preserved versions.
6. [Methodology requirements](methodology-requirements.md): content, calibration provenance, evidence and delivery checklist.
7. [User stories and onboarding](user-stories-onboarding.md): draft workflow, Saved stories and first-run flow (proposed, being worked out).
8. [Strength mock data](mock/strength-mock.md): real saved-export strength fixtures ready for Figma.
9. [Workflow design review](workflow-design-review.md): page map and Discover hierarchy proposal started 2026-10-07; standouts remain pending review.

## How to maintain this memory

- These files record intent and status; source code and inspected payloads establish actual capabilities. Figma establishes the visual design, not a working backend.
- Follow the user's latest explicit decision. If code, Figma and these documents disagree, state the discrepancy rather than silently choosing one.
- Update only the affected documents after work. Include date, scope, source links and test results where relevant.
- Use decision states: proposed, agreed, superseded. Track delivery separately: pending, Figma-only, code implemented, verified, deployed (only with evidence).
- Preserve the reason for a changed decision. Do not erase earlier direction or mark pending items complete because they were discussed.
- This is a baseline from the design conversation and local source inspection. Figma status below reflects the last verified edits in that conversation, not a fresh remote audit on 2026-09-29.
- Do not store credentials, environment-file contents or private connection strings here.

## Immediate handoff

2026-10-07: **Biggest standouts design is pending review** (D82). Existing Discover, full-list and detail prototypes are preserved; Figma delivery does not imply design approval.

2026-10-04 update: the owner requested the real Forecast record page. It is now
implemented locally in the original Next.js frontend at `/forecast-record`, with
typed, authenticated adapters to the Ballistics API. Scoped type/contract tests
and isolated backend tests passed. Browser interactions and desktop/mobile visual
checks now pass with explicitly synthetic intercepted test fixtures; live
configuration, authenticated end-to-end smoke testing and deployment remain pending.
See implementation-status.md for the resolved startup limitation.
This advances only the Forecast record page beyond
the earlier design-first baseline; other frontend scope is unchanged.

Updated 2026-10-02 (docs reconciled against the decision and status logs).

Done in Figma: D06 (league averages removed from Discover cards, kept in details), Methodology access (How it works footer plus contextual links, D31), Forecast record in primary navigation (D34).

Open:
1. **Saved** has no screen yet. Model agreed (D37–D38: fixture shortlist + followed leagues, account from day one); stories in [user-stories-onboarding.md](user-stories-onboarding.md). Navigation placement still open.
2. **Onboarding / first run** is not designed. Draft flow in the same doc.
3. **Strength Discover** uses the engine shortlist at gap > 250 (D43/D44, engine updated). Mock data: [mock/strength-mock.md](mock/strength-mock.md).
5. **Desktop** onboarding, teaser, Saved and Account built (D50). Pricing still undecided ("Price to be announced").
4. **Forecast record backend** (`forecast_history.py`) is deliberately not deployed: new and still in development (D35).

The earlier design-first baseline is superseded only for the locally implemented
Forecast record page described above. No deployment is claimed.
