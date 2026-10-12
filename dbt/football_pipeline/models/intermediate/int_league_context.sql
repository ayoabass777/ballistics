{{config (materialized='table', schema='int')}}

WITH base as (
    SELECT
        country_name,
        league_name,
        l.league_id,
        api_league_id,
        l.tier,
        season,
        ls.league_season_id,
        is_current,
        concat_ws(
            '-',
            lower(regexp_replace(l.league_name, '\\s+', '-', 'g')),
            ls.league_season_id
        ) as league_slug,
        l.logo_file_id as league_logo_file_id,
        c.logo_file_id as country_logo_file_id

    FROM {{ ref('stg_dim_league_seasons') }} ls
    JOIN {{ ref('stg_dim_leagues') }} l
      ON ls.league_id = l.league_id
    JOIN {{ ref('stg_dim_countries') }} c
      ON l.country_id = c.country_id
)

SELECT *
FROM base
