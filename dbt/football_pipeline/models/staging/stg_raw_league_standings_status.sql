-- Standings refresh status per league season (D147): whether the API sent
-- rows that couldn't be loaded and are waiting in the S3 DLQ.
select
    league_season_id,
    api_league_id,
    season,
    status,                                   -- ok | unprocessed | dead
    status <> 'ok' as has_unprocessed_rows,
    unprocessed_rows,
    unprocessed_teams,
    reason,
    unprocessed_since,
    replay_attempts,
    dlq_key,
    updated_at
from {{ source('raw', 'raw_league_standings_status') }}
