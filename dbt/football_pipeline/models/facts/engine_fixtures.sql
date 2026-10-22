{{ config(materialized='view') }}

-- The engine's fixture input: fact_fixtures without knockout games (play-off
-- ties, finals, cup rounds) or post-split phases (championship / relegation /
-- promotion groups, play-off groups: a different energy, owner 2026-10-21).
-- The engine models regular league play; these are neither training data nor
-- prediction targets. Fixtures with no round yet
-- (is_knockout NULL) stay in. Lookups by fixture_id (scoring past picks,
-- match API) read fact_fixtures directly.
select *
from {{ ref('fact_fixtures') }}
where is_knockout is not true
  and is_post_split is not true
