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

  fixture_post_split(phase, api_league_id): TRUE for the phases a league
    plays after its table splits or its season ends: championship /
    relegation / promotion rounds and groups, lower-table rounds, Conference
    League groups and play-off groups. "2nd Phase" is the split in Scotland,
    Wales and China League One but a second tournament in Argentina (128),
    like Clausura, so it counts everywhere except there. Reviewed 2026-10-21
    against every league-format phase in fact_fixtures.
#}
{% macro fixture_phase(round_col) -%}
nullif(regexp_replace(btrim({{ round_col }}), '\s+-\s+[0-9]+$', ''), '')
{%- endmacro %}

{% macro fixture_knockout_name(round_col) -%}
({{ round_col }} ~* '(final|semi|quarter|round of [0-9]|round [0-9]|[0-9](st|nd|rd|th) round|play-?in|decider|[0-9](st|nd|rd|th) place|reclasifica|qualif|relegation/promotion)')
{%- endmacro %}

{% macro fixture_post_split(phase_col, api_league_col) -%}
({{ phase_col }} ~* '(championship|relegation|promotion|lower table|conference league|play-?off)'
 or ({{ phase_col }} = '2nd Phase' and {{ api_league_col }} <> 128))
{%- endmacro %}
