{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Staging model for the ETL-populated dim_countries table
-- Downstream models should ref('stg_dim_countries')
--------------------------------------------------------------------------------

select
  country_id,
  country_name,
  logo_file_id
from {{ source('etl', 'dim_countries') }}
