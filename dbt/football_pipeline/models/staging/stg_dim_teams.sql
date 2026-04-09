

{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Staging model for the ETL-populated dim_teams table
-- Usage: downstream models should ref('stg_dim_teams')
--------------------------------------------------------------------------------

select
  team_id,
  api_team_id,
  team_name,
  logo_file_id,
  venue_id
from {{ source('etl', 'dim_teams') }}
