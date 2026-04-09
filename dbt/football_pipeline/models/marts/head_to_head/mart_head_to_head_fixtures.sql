{{ config(materialized='table') }}

select *
from {{ ref('fact_head_to_head') }}
