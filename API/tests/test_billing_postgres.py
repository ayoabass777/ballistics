"""Opt-in integration test (D143). Use only a disposable database bootstrapped with API/sql/bootstrap_accounts.sql."""
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

URL = os.getenv('BILLING_TEST_DATABASE_URL')


def _access(db, uid):
    return db.execute(text('SELECT has_access FROM app.user_access WHERE user_id=:u'), {'u': uid}).scalar()


@pytest.mark.skipif(not URL, reason='Disposable Postgres URL not configured')
def test_postgres_billing_paywall():
    engine = create_engine(URL)
    alice, bob, missing = [f'billing-test-{uuid4()}' for _ in range(3)]
    now = datetime.now(timezone.utc)
    sub = f'sub_{uuid4().hex}'
    try:
        with Session(engine) as db:
            try:
                for uid in (alice, bob):
                    db.execute(text('INSERT INTO app.app_users (user_id) VALUES (:u)'), {'u': uid})
                db.commit()
                assert _access(db, alice) is False
                db.execute(text("INSERT INTO app.billing_customers (user_id, stripe_customer_id) VALUES (:u, :c)"),
                           {'u': alice, 'c': f'cus_{uuid4().hex}'})
                db.execute(text("""INSERT INTO app.subscriptions
                    (stripe_subscription_id, user_id, stripe_price_id, status, current_period_end)
                    VALUES (:s, :u, 'price_test', 'active', :end)"""),
                           {'s': sub, 'u': alice, 'end': now + timedelta(days=30)})
                db.commit()
                assert _access(db, alice) is True
                assert _access(db, bob) is False
                db.execute(text("UPDATE app.subscriptions SET current_period_end=:end WHERE stripe_subscription_id=:s"),
                           {'s': sub, 'end': now - timedelta(minutes=1)})
                db.commit()
                assert _access(db, alice) is False
                db.execute(text("UPDATE app.subscriptions SET status='canceled', current_period_end=NULL WHERE stripe_subscription_id=:s"),
                           {'s': sub})
                db.commit()
                assert _access(db, alice) is False
                ev = f'evt_{uuid4().hex}'
                db.execute(text("INSERT INTO app.stripe_events (event_id, event_type) VALUES (:e, 'invoice.paid')"), {'e': ev})
                db.commit()
                with pytest.raises(IntegrityError):
                    db.execute(text("INSERT INTO app.stripe_events (event_id, event_type) VALUES (:e, 'invoice.paid')"), {'e': ev})
                db.rollback()
                db.execute(text('DELETE FROM app.stripe_events WHERE event_id=:e'), {'e': ev})
                with pytest.raises(IntegrityError):
                    db.execute(text("""INSERT INTO app.subscriptions
                        (stripe_subscription_id, user_id, stripe_price_id, status)
                        VALUES (:s, :u, 'price_test', 'active')"""), {'s': f'sub_{uuid4().hex}', 'u': missing})
                db.rollback()
                db.execute(text('DELETE FROM app.app_users WHERE user_id=:u'), {'u': alice})
                db.commit()
                for t in ('subscriptions', 'billing_customers'):
                    assert db.execute(text(f'SELECT count(*) FROM app.{t} WHERE user_id=:u'), {'u': alice}).scalar() == 0
            finally:
                db.rollback()
                db.execute(text('DELETE FROM app.app_users WHERE user_id IN (:a,:b)'), {'a': alice, 'b': bob})
                db.commit()
    finally:
        engine.dispose()
