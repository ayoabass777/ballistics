{{ config(materialized='view') }}

-- The engine's fixture input: fact_fixtures without knockout games (play-off
-- ties, finals, cup rounds). The engine models league play; knockouts are
-- neither training data nor prediction targets. Fixtures with no round yet
-- (is_knockout NULL) stay in. Lookups by fixture_id (scoring past picks,
-- match API) read fact_fixtures directly.
select *
from {{ ref('fact_fixtures') }}
where is_knockout is not true
