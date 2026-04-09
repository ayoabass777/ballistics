with source as (
    select *
    from {{ source('raw', 'raw_league_standings') }}
),

standings as (
    select
        league_season_id,
        api_league_id,
        season,
        api_team_id,
        team_name,
        rank,
        points,
        games_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_diff,
        form,
        created_at,
        updated_at
    from source s
)

select * from standings
