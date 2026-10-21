# Crest bootstrap — 2026-10-21

Bootstrap now runs `update_crests` after metadata extraction, on an independent branch gated by the existing metadata-change sensor. Task-level failures retry twice at five-minute intervals; fixtures do not depend on the crest task. Per-image failures retain null references and are logged by the existing downloader; they are eligible on a subsequent run. The metadata sensor remains change-driven, not a daily missing-assets retry sensor. The daily dbt run publishes the new references.

Manual current-asset fill added 304 team crests, 16 league logos and 5 country flags, with no failed images reported. Airflow DagBag verified the task and its dependencies.

The manual dbt refresh of `int_league_context` and `fact_fixtures` completed successfully (2 models, zero errors).
