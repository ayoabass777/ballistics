from datetime import datetime, timezone
import pytest
from engine.eval.consensus_slips import select_rolling_standouts


def fx(fid, kickoff, score=.2):
    return {'fixture_id': fid, 'kickoff': kickoff,
            'discover_pick': {'list': 'patterns', 'score': score}}


def test_local_calendar_week_and_started_boundary():
    now = datetime.fromisoformat('2026-10-07T02:00:00+00:00')
    data = [fx(1, now.isoformat()), fx(2, '2026-10-07T03:00:00Z'),
            fx(3, '2026-10-13T06:59:59Z', .3), fx(4, '2026-10-13T07:00:00Z'),
            fx(5, '2026-10-07T02:00:00'), fx(6, 'bad'), fx(7, '2026-10-08T12:00:00Z', .1499)]
    result = select_rolling_standouts(data, now=now, timezone_name='America/Vancouver')
    assert (result['start_date'], result['end_date']) == ('2026-10-06', '2026-10-12')
    assert [r['fixture_id'] for r in result['items']] == [3, 2]
    assert result['items'][1]['fixture_date'] == '2026-10-06'


def test_refresh_removes_started_fixture_and_has_no_cap_or_duplicates():
    data = [fx(i, '2026-10-08T12:00:00Z', .15) for i in range(9)]
    data.append(data[0])
    assert select_rolling_standouts(data, now=datetime(2026,10,7,tzinfo=timezone.utc))['count'] == 9
    assert select_rolling_standouts(data, now=datetime(2026,10,8,12,tzinfo=timezone.utc))['count'] == 0


def test_dst_uses_calendar_days_not_168_hours():
    # Los Angeles falls back on 2026-11-01, so this week is 169 hours long.
    # The window must end at local midnight on day 7; now + 168h would end
    # at 23:00 on Nov 6 and drop late kickoffs.
    result=select_rolling_standouts([], now=datetime.fromisoformat('2026-10-31T12:00:00-07:00'), timezone_name='America/Los_Angeles')
    assert result['end_exclusive'] == '2026-11-07T00:00:00-08:00'


def test_vancouver_stays_on_utc_minus_7_after_november_2026():
    # BC stops changing clocks after 2026-11-01 (tzdata 2026c+). Fails on
    # older tz data, which would show BC kickoffs an hour off.
    result=select_rolling_standouts([], now=datetime.fromisoformat('2026-10-31T12:00:00-07:00'), timezone_name='America/Vancouver')
    assert result['end_exclusive'] == '2026-11-07T00:00:00-07:00'


def test_naive_reference_time_rejected():
    with pytest.raises(ValueError): select_rolling_standouts([], now=datetime(2026,10,6))
