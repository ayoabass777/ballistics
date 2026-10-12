{#
  Fixture stage helpers over API-Football league.round.

  fixture_phase(round): the round without its matchday number
    "Regular Season - 12" -> "Regular Season", "Relegation Group - 3" ->
    "Relegation Group", "Semi-finals" -> "Semi-finals". NULL if no round.

  fixture_knockout_name(round): TRUE when the label itself names a tie or
    one-off game (finals, semis, quarters, "Round of 16", "MLS Cup - Round 1",
    "1st Round", play-in, deciders, place games, reclassification,
    qualification). Other phases are decided by format in fact_fixtures:
    round-robin when a team plays 3+ games in the phase, knockout otherwise.
    Labels reviewed 2026-10-12 against all 465 distinct rounds:
    etl/sql/diagnostics/fixture_rounds.sql.
#}
{% macro fixture_phase(round_col) -%}
nullif(regexp_replace(btrim({{ round_col }}), '\s+-\s+[0-9]+$', ''), '')
{%- endmacro %}

{% macro fixture_knockout_name(round_col) -%}
({{ round_col }} ~* '(final|semi|quarter|round of [0-9]|round [0-9]|[0-9](st|nd|rd|th) round|play-?in|decider|[0-9](st|nd|rd|th) place|reclasifica|qualif|relegation/promotion)')
{%- endmacro %}
