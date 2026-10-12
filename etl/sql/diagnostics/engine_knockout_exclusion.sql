-- What the engine no longer sees: knockout games by phase, and totals.
select count(*) filter (where is_knockout) as knockout_fixtures,
       count(*) filter (where is_knockout and fixture_status = 'NS') as upcoming_knockouts,
       count(*) as all_fixtures,
       (select count(*) from raw.engine_fixtures) as engine_fixtures,
       (select count(*) from raw.fact_head_to_head) as h2h_rows,
       (select count(*) from raw.engine_head_to_head) as engine_h2h_rows
from raw.fact_fixtures;

select league_name, phase, kickoff_utc::date as kickoff, home_team_name, away_team_name
from raw.fact_fixtures
where is_knockout and fixture_status = 'NS'
order by kickoff_utc
limit 20;
