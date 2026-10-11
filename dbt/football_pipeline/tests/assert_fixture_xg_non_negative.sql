-- Expected goals can never be negative.
select fixture_id, home_xg, away_xg
from {{ ref('fact_fixture_xg') }}
where home_xg < 0 or away_xg < 0
