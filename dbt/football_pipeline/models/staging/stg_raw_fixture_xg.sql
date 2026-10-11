-- Staged expected goals: rename/cast only. One row per fixture that has xG.
-- View (not incremental): xG often lands days after the result and does not
-- touch raw_fixtures.updated_at, so downstream must always see the latest rows.
select
    fixture_id,
    api_fixture_id,
    home_xg::double precision as home_xg,
    away_xg::double precision as away_xg,
    captured_at
from {{ source('raw', 'fixture_xg') }}
