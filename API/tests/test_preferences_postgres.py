"""Opt-in integration test. Use only a disposable database bootstrapped with API/sql/bootstrap_accounts.sql."""
import os
from uuid import uuid4
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from API.preferences import read_preferences, save_preferences


@pytest.mark.skipif(not os.getenv('PREFERENCES_TEST_DATABASE_URL'), reason='Disposable Postgres URL not configured')
def test_postgres_preferences():
    engine = create_engine(os.environ['PREFERENCES_TEST_DATABASE_URL'])
    alice, bob, missing = [f'preferences-test-{uuid4()}' for _ in range(3)]
    try:
        with Session(engine) as db:
            try:
                for uid in (alice, bob):
                    db.execute(text('INSERT INTO app.app_users (user_id) VALUES (:uid)'), {'uid': uid})
                assert read_preferences(db, alice)['timezone_override'] is None
                first = save_preferences(db, alice, 'Europe/London')
                db.commit()
                assert read_preferences(db, alice)['timezone_override'] == 'Europe/London'
                assert read_preferences(db, bob)['timezone_override'] is None
                reset = save_preferences(db, alice, None)
                db.commit()
                assert reset['created_at'] == first['created_at']
                assert reset['updated_at'] >= first['updated_at']
                assert read_preferences(db, alice)['timezone_override'] is None
                with pytest.raises(IntegrityError):
                    save_preferences(db, missing, 'UTC')
                db.rollback()
                save_preferences(db, alice, 'Asia/Tokyo')
                db.execute(text('DELETE FROM app.app_users WHERE user_id=:uid'), {'uid': alice})
                db.commit()
                assert db.execute(text('SELECT count(*) FROM app.user_preferences WHERE user_id=:uid'), {'uid': alice}).scalar() == 0
            finally:
                db.rollback()
                db.execute(text('DELETE FROM app.app_users WHERE user_id IN (:alice,:bob)'), {'alice':alice,'bob':bob})
                db.commit()
    finally:
        engine.dispose()
