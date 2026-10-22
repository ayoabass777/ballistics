{{ config(materialized='view') }}

-- The engine's team-match history: fact_head_to_head without knockout games
-- or post-split phases (fact_fixtures.is_post_split).
-- Feeds team strength, Elo, league rates, streaks and calibration. Product
-- H2H (match API, marts) keeps reading fact_head_to_head with every game.
select h.*
from {{ ref('fact_head_to_head') }} as h
join {{ ref('fact_fixtures') }} as f
  on f.fixture_id = h.fixture_id
where f.is_knockout is not true
  and f.is_post_split is not true
