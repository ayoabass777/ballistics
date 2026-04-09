{{ config(
    materialized='table',
    schema='mart',
    post_hook=[
      "CREATE EXTENSION IF NOT EXISTS pg_trgm",
      "CREATE INDEX IF NOT EXISTS mart_dim_search_name_trgm ON {{ this }} USING GIN (name_lc gin_trgm_ops)"
    ]
) }}

select 
    entity_type,
    entity_id,
    name,
    name_lc,
    logo_file_id
from {{ ref('int_dim_search') }}
