-- Known API-Football round labels: phase extraction and knockout-by-name.
-- (Format-based stage for the remaining phases is checked on real data in
-- fact_fixtures via accepted_values and the diagnostics query.)
with cases(round, phase, knockout_name) as (
    values
        ('Regular Season - 12', 'Regular Season', false),
        ('Relegation Group - 3', 'Relegation Group', false),
        ('Conference League Play-off Group - 4', 'Conference League Play-off Group', false),
        ('Relegation Round', 'Relegation Round', false),
        ('Group Stage', 'Group Stage', false),
        ('Promotion Play-offs - Semi-finals', 'Promotion Play-offs - Semi-finals', true),
        ('MLS Cup - Round 1', 'MLS Cup - Round 1', true),
        ('Round of 16', 'Round of 16', true),
        ('Final', 'Final', true),
        ('1st Phase - 8th Finals', '1st Phase - 8th Finals', true),
        ('Apertura - Play-In Final', 'Apertura - Play-In Final', true),
        ('Apertura - Reclasificación', 'Apertura - Reclasificación', true),
        ('3rd place', '3rd place', true),
        ('Championship Decider', 'Championship Decider', true),
        ('1st Round', '1st Round', true),
        ('Additional Qualification', 'Additional Qualification', true)
)
select round, phase, knockout_name,
       {{ fixture_phase('round') }} as got_phase,
       {{ fixture_knockout_name('round') }} as got_knockout_name
from cases
where {{ fixture_phase('round') }} is distinct from phase
   or {{ fixture_knockout_name('round') }} is distinct from knockout_name
