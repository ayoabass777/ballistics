-- Every distinct round label with its derived stage, to review the
-- fixture_stage macro after the backfill. Knockout labels first.
select stage, round, count(*) as fixtures,
       count(distinct league_id) as leagues,
       min(season) as first_season, max(season) as last_season
from raw.fact_fixtures
group by 1, 2
order by stage desc nulls first, fixtures desc;

-- Coverage: fixtures still without a round.
select count(*) filter (where round is null) as no_round, count(*) as total
from raw.fact_fixtures;
