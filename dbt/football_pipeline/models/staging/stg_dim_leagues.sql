{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Staging model for the ETL-populated dim_leagues table
-- Downstream models should ref('stg_dim_leagues')
--------------------------------------------------------------------------------

select
  league_id,
  api_league_id,
  league_name,
  country_id,
  tier,
  logo_file_id
from {{ source ('etl', 'dim_leagues') }}
