{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_league_teams
-- Per-league-season team list with rank/form for page contexts.
--------------------------------------------------------------------------------

select
    team_id,
    team_name,
    team_slug,
    rank        as team_rank,
    form        as team_form,
    logo_file_id,
    league_season_id
from {{ ref('mart_team_info') }}
