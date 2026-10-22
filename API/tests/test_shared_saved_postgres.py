"""Opt-in, transaction-rolled-back PostgreSQL tests; no test rows survive."""
import io
import os
from pathlib import Path
from uuid import uuid4
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from API.core.database import engine
from API.account_data import SavedCreate, ensure_account_user, insert_saved, list_saved, delete_saved
from API.saved_snapshots import archived_card
from scripts.archive_saved_snapshots import archive_partition

CARD = {'fixture_id': 987654321, 'home': 'Home', 'away': 'Away', 'league': 'League',
        'league_id': None, 'country': None, 'kickoff': '2026-10-25T12:00:00Z', 'pick': None}

class S3:
    def __init__(self): self.objects = {}; self.corrupt = False
    def put_object(self, Bucket, Key, Body, **kw): self.objects[Key] = Body
    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(b'corrupt' if self.corrupt else self.objects[Key])}

@pytest.mark.skipif(os.getenv('TEST_ACCOUNT_POSTGRES') != '1', reason='Explicit local DB test opt-in required')
def test_shared_storage_and_archive(monkeypatch):
    with engine.connect() as db:
        tx = db.begin()
        try:
            db.execute(text(Path('API/sql/shared_saved_snapshots.sql').read_text()))
            alice, bob = ['snapshot-test-'+str(uuid4()) for _ in range(2)]
            for uid in (alice, bob): ensure_account_user(db, uid)
            item = SavedCreate(card=CARD, run_ds=str(uuid4()))
            for uid in (alice, bob): insert_saved(db, uid, item)
            ids = db.execute(text('SELECT saved_snapshot_id FROM app.saved_fixture_refs WHERE user_id IN (:a,:b)'), {'a': alice, 'b': bob}).scalars().all()
            assert len(ids) == 2 and ids[0] == ids[1]
            before = list_saved(db, alice)
            insert_saved(db, alice, SavedCreate(card={**CARD, 'home': 'Revised'}, run_ds='new'))
            assert list_saved(db, alice) == before
            delete_saved(db, alice, CARD['fixture_id'])
            assert list_saved(db, alice) == [] and len(list_saved(db, bob)) == 1
            insert_saved(db, alice, SavedCreate(card={**CARD, 'home': 'Revised'}, run_ds='new'))
            assert list_saved(db, alice)[0]['card']['home'] == 'Revised'
            with pytest.raises(DBAPIError):
                with db.begin_nested(): db.execute(text('UPDATE app.saved_snapshot_payloads SET card=\'{}\' WHERE snapshot_id=:id'), {'id': ids[0]})
            with pytest.raises(DBAPIError):
                with db.begin_nested(): db.execute(text('UPDATE app.saved_snapshots SET run_ds=\'bad\' WHERE id=:id'), {'id': ids[0]})
            # Separate ancient partition so real current saves can never be retired.
            db.execute(text("CREATE TABLE app.saved_snapshot_payloads_190001 PARTITION OF app.saved_snapshot_payloads FOR VALUES FROM ('1900-01-01') TO ('1900-02-01')"))
            from API.saved_snapshots import snapshot_identity, canonical
            card = SavedCreate(card=CARD).card.model_dump(mode='json')
            old = db.execute(text('''INSERT INTO app.saved_snapshots(fixture_id, content_hash, captured_at, capture_month, provenance, run_ds)
                VALUES (:fixture, :hash, '1900-01-02', '1900-01-01', 'legacy_card_v1', 'ancient') RETURNING id'''),
                {'fixture': CARD['fixture_id'], 'hash': snapshot_identity(card, 'ancient', 'legacy_card_v1')}).scalar_one()
            db.execute(text("INSERT INTO app.saved_snapshot_payloads VALUES ('1900-01-01', :id, CAST(:card AS jsonb))"), {'id':old, 'card':canonical(card)})
            s3 = S3(); monkeypatch.setenv('SAVED_ARCHIVE_BUCKET','private-test')
            assert archive_partition(db,'1900-01-01',90,s3,True,True)['blocked'] == 1
            db.execute(text("UPDATE app.saved_snapshots SET retention_hold=FALSE, settled_at='1900-01-03' WHERE id=:id"), {'id':old})
            s3.corrupt = True
            with pytest.raises(ValueError): archive_partition(db,'1900-01-01',90,s3,True,True)
            assert db.execute(text('SELECT count(*) FROM app.saved_snapshot_payloads WHERE snapshot_id=:id'),{'id':old}).scalar_one()==1
            s3.corrupt = False
            assert archive_partition(db,'1900-01-01',90,s3,True,True)['verified']
            row=db.execute(text('SELECT * FROM app.saved_snapshots WHERE id=:id'),{'id':old}).mappings().one()
            assert archived_card(row,s3)==card
            assert archive_partition(db,'1900-01-01',90,s3,True,True)['rows']==0
        finally: tx.rollback()
