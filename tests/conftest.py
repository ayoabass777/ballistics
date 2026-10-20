"""Shared test setup.

etl/src/config.py loads .env on import, which holds the real SMTP and Slack
settings, so any test that reaches notify.send_alert without patching it would
send a real alert. Swap the senders for no-ops in every test; tests that check
sending patch notify.SENDERS themselves, on top of this.
"""
from unittest.mock import patch

import pytest

from etl.src.data_detector import notify


@pytest.fixture(autouse=True)
def no_real_alerts():
    with patch.dict(notify.SENDERS, {name: lambda *a, **k: None for name in notify.SENDERS}):
        yield
