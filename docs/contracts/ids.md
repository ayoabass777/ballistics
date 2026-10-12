# ID rule

Updated 2026-10-11.

Two ID spaces exist. API-Football ids (`api_fixture_id`, `api_league_id`, `api_team_id`) come from the provider. Warehouse ids (`fixture_id`, `team_id`, `league_id`, `league_season_id`) are ours.

1. **API ids stop at dbt staging.** They exist in `raw.*` ETL tables, in `stg_*` models, and in the mapping tables (`stg_dim_teams`, `stg_dim_leagues`, `dim_teams`). Staging names them explicitly (`home_api_team_id`, never `home_team_id`).
2. **Everything after staging uses warehouse ids**: facts, marts, the engine, engine exports and the API.
3. **The engine reads dbt only.** Fixtures come from `raw.fact_fixtures`; it never reads `raw.raw_fixtures` (guarded by `tests/test_fact_fixtures_source.py`). No module rebuilds an API → warehouse map.
4. **Nothing API-Football-specific reaches the frontend.** Exports carry `home_team_id`, `away_team_id`, `league_id` (warehouse). Crests use `logo_file_id`, not provider image URLs.

## Known exceptions (to remove)

- `raw.fact_fixtures.api_league_id`: engine calibration maps, league clustering and the Big dog state are keyed on it. Move them to `league_id`, then drop the column.
- Engine reads of `dim.dim_leagues` / `dim.dim_countries` / `dim.dim_league_seasons` (ETL tables) for league names, tiers and seasons. Replace with dbt models.

## History

2026-10-11 — `fact_fixtures` added; staging renamed `home_team_id`/`away_team_id` to `home_api_team_id`/`away_api_team_id`; 7 copies of the API → warehouse team map removed from the engine. Side effect: live Poisson and its walk-forward fold had been merging team strength on API ids against warehouse ids, so upcoming fixtures got league-mean strength (or another club's, on an id collision). They now get the right team's strength; refit calibration after deploying. `engine/eval/diagnostics/fixtures_lineage_check.py` quantifies the change.
