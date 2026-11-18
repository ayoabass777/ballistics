from copy import deepcopy
from engine.api.match_detail import retain_matches_pattern


def test_retains_exact_highlight_without_mutating_snapshot():
    view = {k: [] for k in ('top_results', 'top_attack_defence', 'also', 'see_all')}
    source = {'discovery': {'fixture': {'home': 'Coritiba', 'away': 'Santos'},
        'sections': {'patterns': {'list': 'patterns', 'team': 'Santos', 'event': 'Over_2_5',
            'context': 'away', 'k': 14, 'n': 17, 'score': .2}}},
        'patterns': {'default_view': 'all', 'all': deepcopy(view), 'venue': deepcopy(view)}}
    before = deepcopy(source)
    result = retain_matches_pattern(source)
    row = result['venue']['also'][0]
    assert (row['team'], row['event'], row['context'], row['k'], row['n']) == ('Santos', 'Over_2_5', 'away', 14, 17)
    assert result['default_view'] == 'venue'
    assert row['league_rate'] is None
    assert source == before
    source['patterns'] = result
    assert retain_matches_pattern(source) == result
    source['patterns']['venue']['see_all'] = source['patterns']['venue']['also']
    source['patterns']['venue']['also'] = []
    promoted = retain_matches_pattern(source)
    assert len(promoted['venue']['also']) == 1
    assert not promoted['venue']['see_all']


def test_non_pattern_highlight_does_not_change_patterns():
    source = {'patterns': None, 'discovery': {'sections': {'goals': {
        'list': 'goals', 'score': .4, 'probability': .8}}}}
    assert retain_matches_pattern(source) is None
