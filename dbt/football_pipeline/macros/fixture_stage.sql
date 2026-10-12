{#
  Competition stage from API-Football league.round. Returns 'league' or
  'knockout' (NULL when the round is unknown, i.e. not yet backfilled).

    league   : round-robin play. Rounds numbered "<phase> - <n>"
               ("Regular Season - 12", "Championship Round - 3",
               "Group A - 2", "Apertura - 5", "League Stage - 8"), or named
               regular season / group / league stage.
    knockout : single games or ties: anything with play-off / final /
               knockout in the name ("Play-offs", "Semi-finals",
               "Knockout Round Play-offs", "8th Finals"), and remaining cup
               rounds ("Round of 16", "1st Round", "Round One",
               "3rd Qualifying Round").

  Order matters: play-off/final wins over a trailing matchday number
  ("Relegation Play-offs - 1" is knockout). Review new labels with
  etl/sql/diagnostics/fixture_rounds.sql.
#}
{% macro fixture_stage(round_col) -%}
case
    when {{ round_col }} is null or btrim({{ round_col }}) = '' then null
    when {{ round_col }} ~* '(play-?offs?|final|knock-?out)' then 'knockout'
    when {{ round_col }} ~ ' - [0-9]+$' then 'league'
    when {{ round_col }} ~* '(regular season|group|league stage|league phase)' then 'league'
    else 'knockout'
end
{%- endmacro %}
