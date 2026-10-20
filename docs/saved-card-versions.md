# Saved card status and versions

Implemented locally, 2026-10-20. Source: `engine/eval/card_versions.py`, `engine/eval/match_store.py`; original repository `API/card_identity.py`, `API/saved_snapshots.py`, `frontend_dev/components/research/SavedPage.tsx`.

## Bootstrap and daily publication

Run `python -m scripts.backfill_card_versions` from Ballistics before enabling the account response changes. This creates `raw.card_versions` and initializes an empty history from retained completed match runs using original `written_at` timestamps. It refuses replay into a populated history. Replaced historical daily runs cannot be reconstructed. Existing local history is initialized.

Subsequent `write_match_analysis` calls publish card changes in the same transaction as the complete match run. An advisory transaction lock serializes version assignment. Each context has increasing version numbers; identical consecutive hashes reuse the previous version, including across run dates. Older logical runs cannot supersede a newer version in that context. Rows are append-only by application convention, not SQL triggers. TBD fixtures without kickoff are skipped until a valid kickoff exists.

## Identity and response

Context is schema version plus fixture, list, team, event, context and market. Content includes normalized UTC kickoff and all public pick fields. Cosmetic names/logos, run dates and private scores are excluded. Strength side/label changes remain meaningful content; different markets are different contexts. Engine and account identity functions must remain identical; update both with a schema change when semantics change.

The existing saved snapshot hash and archive checksum remain unchanged for integrity. They are separate from the new meaningful-content hash. No user reference or historical payload is rewritten.

Saved returns `snapshot_id`, `status`, `current_kickoff`, `card_version`, `content_hash`, `latest_version`, `updated_since_saved` alongside its original card/run/save date. A numbered baseline requires equal content observed no later than the save. A legacy unmatched card remains unnumbered; it may show a later changed comparable hash without inventing a baseline version. A→B→A after a known saved A is an update event despite matching final content.

Status is read from `raw.fact_fixtures.fixture_status`, never inferred from elapsed kickoff. Missing fixtures show unknown; postponed, cancelled, interrupted, abandoned and suspended are preserved separately. Data freshness remains dependent on ingestion. Current kickoff is shown separately when rescheduled.

## Verification and limits

16 focused account and identity tests passed; scoped frontend TypeScript and account proxy checks passed. Existing live saves resolved without mutation. A rolled-back PostgreSQL test verified identical-content reuse and A→B→A numbering. Account API restarted. Browser visual inspection unavailable because the Mac was locked.

No retention, pagination, archive deletion, automatic save replacement or Figma changes in this step. Versions cover published comparable cards; disappearance from a shortlist is not a new evidence version. The account read currently loads history for saved fixture IDs; cursor pagination and bounded version queries remain follow-up work.

## Pagination update — 2026-10-20

Supersedes the unbounded-read limitation above. Saved uses `/account/saved/page?scope=all&limit=12&cursor=...`, returning `{data:{items,next_cursor}}`. Maximum page size is 50. The cursor encodes the filter plus last saved timestamp and fixture ID; it is validated and never grants access outside the authenticated user. Newer saves do not shift existing page boundaries. Current status may change between requests; filter membership is live rather than a frozen database snapshot.

All/Upcoming/Live/Finished/Other statuses filter before limiting. Unknown and exceptional statuses remain accessible under Other and All. Payload/archive resolution and version queries apply only to the requested page. Two limited version queries per card replace full-history loading. Save buttons use `/account/saved/ids` returning `{data:{fixture_ids}}`, avoiding loading full card payloads for membership. The lightweight ID list is still proportional to the account shortlist; it is not paginated. Old list and mutation response shapes remain arrays but are capped at 12; consumers must use the paged endpoint to enumerate cards.

Verified with 27 synthetic saves, equal timestamps, user isolation, invalid/mismatched cursors, insertion between requests, and current NS/FT status filters in a rolled-back local PostgreSQL transaction. Browser All and Finished states verified against existing user saves. Load-more layout with more than 12 real user saves was not exercised in the browser.

## Session return state and final browser checks

`useSavedBrowse` stores selected filter and per-filter page count/scroll position under the account key in sessionStorage. It stores no card payloads. Opening a match persists the current position; returning refetches the same number of pages, then restores scroll after rendering. Filter changes abort pending requests and keep independent positions. Loading more appends unique fixture IDs. Storage failure falls back to fresh browsing. Memory is session-local, not an account preference.

On 20 Oct, the isolated `node frontend_dev/scripts/saved-preview.cjs` harness on port 3104 verified 12→24→27 cards with no remaining Load more button, then Match→Back. Mobile Upcoming retained 24 cards and scroll Y 7748.5 after returning. Layout had no horizontal document overflow at 390px and 320px. These tests used explicitly labelled synthetic data and a test account, without changing real user saves. Frontend/API commit: `891a2f2`.

## Figma handoff

The existing [desktop Saved prototype](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-51257&starting-point-node-id=334%3A51257) now links five filter states and shows version/update language. Historical examples remain labelled illustrative. The [pagination handoff](https://www.figma.com/design/dLgWAaafmzxQNcp2Rb9P7u?node-id=1972-63681) is a static specimen describing runtime behavior; it does not append mock fixtures. Original content remains hidden at `1970:63170`. The historical stacked sample-card layout and existing detail links remain, so this is a semantics/control sync rather than a pixel-identical site-grid export. Mobile Figma was not changed.
