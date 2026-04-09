{{ config(materialized='table') }}

-------------------------------------------------------------------------------
-- Mart: mart_nav_leagues
-- Minimal league metadata for navigation (country + logos + season key).
-------------------------------------------------------------------------------

select
    country_name,
    league_name,
    country_logo_file_id,
    league_logo_file_id,
    league_season_id
from {{ ref('int_league_context') }}
