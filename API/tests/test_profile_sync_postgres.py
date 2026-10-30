"""Opt-in integration test. Use only a disposable database bootstrapped with API/sql/bootstrap_accounts.sql."""
import os
from unittest.mock import patch
from uuid import uuid4
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from API import app as api


@pytest.mark.skipif(not os.getenv('PROFILE_TEST_DATABASE_URL'), reason='Disposable Postgres URL not configured')
def test_failed_clerk_lookup_keeps_the_stored_profile():
    engine = create_engine(os.environ['PROFILE_TEST_DATABASE_URL'])
    uid = f'profile-test-{uuid4()}'
    try:
        with Session(engine) as db:
            try:
                # Usage events can create the row before /me has a profile.
                db.execute(text('INSERT INTO app.app_users (user_id) VALUES (:uid)'), {'uid': uid})
                profile = {'email': 'member@example.com', 'first_name': 'Ada',
                           'last_name': 'Lovelace', 'image_url': 'https://example.com/a.png'}
                with patch.object(api, '_fetch_clerk_user_profile', return_value=profile):
                    row = api._upsert_app_user_from_claims(db, {'sub': uid})
                db.commit()
                assert row['email'] == 'member@example.com'
                with patch.object(api, '_fetch_clerk_user_profile', return_value={}):
                    row = api._upsert_app_user_from_claims(db, {'sub': uid})
                db.commit()
                assert (row['email'], row['first_name'], row['last_name'], row['image_url']) == (
                    'member@example.com', 'Ada', 'Lovelace', 'https://example.com/a.png')
                with patch.object(api, '_fetch_clerk_user_profile', return_value={**profile, 'email': 'new@example.com'}):
                    row = api._upsert_app_user_from_claims(db, {'sub': uid})
                db.commit()
                assert row['email'] == 'new@example.com'
            finally:
                db.rollback()
                db.execute(text('DELETE FROM app.app_users WHERE user_id=:uid'), {'uid': uid})
                db.commit()
    finally:
        engine.dispose()
