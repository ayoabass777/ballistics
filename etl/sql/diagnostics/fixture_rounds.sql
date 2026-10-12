-- Competition phases (round minus matchday number) with their derived stage,
-- to review fixture_phase / fixture_knockout_name and the format rule.
select stage, is_playoff, phase, count(*) as fixtures,
       count(distinct league_season_id) as league_seasons,
       round(count(*)::numeric / nullif(count(distinct league_season_id), 0), 1) as per_season
from raw.fact_fixtures
group by 1, 2, 3
order by stage desc nulls first, is_playoff desc, fixtures desc;

-- Coverage: fixtures still without a round.
select count(*) filter (where round is null) as no_round, count(*) as total
from raw.fact_fixtures;
