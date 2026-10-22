"""Opt-in PostgreSQL check; all synthetic account writes are rolled back."""
import os
from uuid import uuid4
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from API.account_data import ensure_account_user, list_followed_leagues, replace_followed_leagues

@pytest.mark.skipif(not os.getenv('FOLLOWED_LEAGUES_TEST_DATABASE_URL'), reason='Postgres test URL not configured')
def test_follow_storage():
    engine = create_engine(os.environ['FOLLOWED_LEAGUES_TEST_DATABASE_URL'])
    try:
        with engine.connect() as db:
            transaction = db.begin()
            try:
                ids=db.execute(text('SELECT league_id FROM dim.dim_leagues ORDER BY league_id LIMIT 2')).scalars().all()
                assert len(ids)==2, 'Requires two catalog leagues'
                alice,bob=[f'follow-test-{uuid4()}' for _ in range(2)]
                ensure_account_user(db,alice);ensure_account_user(db,bob)
                replace_followed_leagues(db,alice,ids)
                original=db.execute(text('SELECT created_at FROM app.user_followed_leagues WHERE user_id=:uid AND league_id=:lid'),{'uid':alice,'lid':ids[0]}).scalar_one()
                replace_followed_leagues(db,alice,ids)
                assert list_followed_leagues(db,alice)==ids
                assert list_followed_leagues(db,bob)==[]
                replace_followed_leagues(db,alice,[ids[0]])
                assert db.execute(text('SELECT created_at FROM app.user_followed_leagues WHERE user_id=:uid'),{'uid':alice}).scalar_one()==original
                with pytest.raises(ValueError):replace_followed_leagues(db,alice,[9223372036854775807])
                assert list_followed_leagues(db,alice)==[ids[0]]
                with pytest.raises(IntegrityError):
                    with db.begin_nested():
                        db.execute(text('INSERT INTO app.user_followed_leagues (user_id,league_id) VALUES (:uid,9223372036854775807)'),{'uid':alice})
                replace_followed_leagues(db,alice,[]);assert list_followed_leagues(db,alice)==[]
                replace_followed_leagues(db,alice,ids)
                db.execute(text('DELETE FROM app.app_users WHERE user_id=:uid'),{'uid':alice})
                assert list_followed_leagues(db,alice)==[]
            finally:transaction.rollback()
    finally:engine.dispose()
