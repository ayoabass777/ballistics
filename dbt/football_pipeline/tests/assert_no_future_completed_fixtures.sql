-- Singular test: no fixture should be marked as played (FT) with a kickoff in the future.
-- This catches data quality issues where the API returns a "finished" status
-- for a match that hasn't happened yet — typically a sync bug or timezone mismatch.

SELECT
    fixture_id,
    api_fixture_id,
    kickoff_utc,
    fixture_status
FROM {{ ref('stg_raw_fixtures') }}
WHERE is_played = TRUE
  AND kickoff_utc > NOW()
