{{ config(severity='warn') }}
-- Singular test: no fixture should be marked as played (FT) with a kickoff in the future.
-- This catches data quality issues where the API returns a "finished" status
-- for a match that hasn't happened yet — typically a sync bug or timezone mismatch.
-- A warning, not an error: the rows mirror the provider, which we cannot fix
-- at source (Nov 2026: Shakhtar v Livyi Bereh, Nov 20 result filed on the
-- May 2027 fixture), and an error would block every deploy and dbt build.

SELECT
    fixture_id,
    api_fixture_id,
    kickoff_utc,
    fixture_status
FROM {{ ref('stg_raw_fixtures') }}
WHERE is_played = TRUE
  AND kickoff_utc > NOW()
