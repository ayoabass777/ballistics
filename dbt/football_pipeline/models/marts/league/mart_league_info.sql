{{ config(materialized='table') }}

-------------------------------------------------------------------------------
-- Mart: mart_league_info
-- League season metadata with friendly slug and country.
-------------------------------------------------------------------------------

select
    league_season_id,
    league_name,
    league_slug,
    season as season_label,
    country_name
from {{ ref('int_league_context') }}
