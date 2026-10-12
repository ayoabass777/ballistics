-- Known API-Football round labels must classify as expected.
with cases(round, expected) as (
    values
        ('Regular Season - 12', 'league'),
        ('Championship Round - 3', 'league'),
        ('Relegation Round - 2', 'league'),
        ('Group A - 2', 'league'),
        ('Apertura - 5', 'league'),
        ('League Stage - 8', 'league'),
        ('Group Stage', 'league'),
        ('Play-offs', 'knockout'),
        ('Relegation Play-offs - 1', 'knockout'),
        ('Knockout Round Play-offs', 'knockout'),
        ('Round of 16', 'knockout'),
        ('Quarter-finals', 'knockout'),
        ('Semi-finals', 'knockout'),
        ('Final', 'knockout'),
        ('8th Finals', 'knockout'),
        ('1st Round', 'knockout'),
        ('Round One', 'knockout'),
        ('3rd Qualifying Round', 'knockout')
)
select round, expected, {{ fixture_stage('round') }} as got
from cases
where {{ fixture_stage('round') }} is distinct from expected
