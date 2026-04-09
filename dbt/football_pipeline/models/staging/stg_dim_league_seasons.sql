{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Staging model for the ETL-populated dim_league_seasons table
-- Adds is_current column to indicate if the current date falls within the season
-- Downstream models should ref('stg_dim_league_seasons')
--------------------------------------------------------------------------------

select
  league_season_id,
  league_id,
  season,
  season_label,
  start_date,
  end_date,
  CASE 
    WHEN (NOW()::date BETWEEN start_date AND end_date) THEN TRUE 
   ELSE FALSE
   END AS is_current
from {{ source('etl', 'dim_league_seasons') }}
