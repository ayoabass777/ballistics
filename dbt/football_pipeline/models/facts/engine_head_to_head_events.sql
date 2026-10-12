{{ config(materialized='view') }}

-- The engine's per-fixture event flags (patterns, streak beliefs, league base
-- rates): fact_head_to_head_events without knockout games. The engine rebuilds
-- runs from these ordered flags, so dropping rows drops knockouts from runs.
select e.*
from {{ ref('fact_head_to_head_events') }} as e
join {{ ref('fact_fixtures') }} as f
  on f.fixture_id = e.fixture_id
where f.is_knockout is not true
