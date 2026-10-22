"""Opt-in verification; synthetic account and saves are rolled back."""
import os
from datetime import datetime, timezone
from uuid import uuid4
import pytest
from sqlalchemy import text
from API.core.database import engine
from API.account_data import ensure_account_user, insert_saved, SavedCreate
from API.saved_snapshots import read_saved_page, saved_ids

@pytest.mark.skipif(os.getenv('TEST_ACCOUNT_POSTGRES') != '1', reason='Local database opt-in')
def test_pages_filters_and_ties():
    with engine.connect() as db:
        tx = db.begin()
        try:
            user='paging-test-'+str(uuid4()); ensure_account_user(db,user)
            stamp=datetime.now(timezone.utc)
            def save(fid):
                insert_saved(db,user,SavedCreate(card=dict(fixture_id=fid,home='Home',away='Away',league='Test',kickoff='2099-01-01T12:00:00Z'),run_ds=None),stamp)
            for fid in range(990001000,990001027): save(fid)
            first=read_saved_page(db,user,limit=12)
            second=read_saved_page(db,user,cursor=first['next_cursor'],limit=12)
            third=read_saved_page(db,user,cursor=second['next_cursor'],limit=12)
            ids=[i['card']['fixture_id'] for page in [first,second,third] for i in page['items']]
            assert len(ids)==27 and len(set(ids))==27 and ids==sorted(ids,reverse=True)
            assert third['next_cursor'] is None
            assert len(saved_ids(db,user)['fixture_ids'])==27
            assert read_saved_page(db,user,'upcoming')['items']==[]
            assert len(read_saved_page(db,user,'other')['items'])==12
            assert read_saved_page(db,'different-user')['items']==[]
            for cursor in ['broken',first['next_cursor']]:
                with pytest.raises(ValueError): read_saved_page(db,user,'finished',cursor)
            for status,scope in [('NS','upcoming'),('FT','finished')]:
                fid=db.execute(text('SELECT fixture_id FROM raw.fact_fixtures WHERE fixture_status=:status LIMIT 1'),{'status':status}).scalar_one()
                save(fid)
                page=read_saved_page(db,user,scope)
                assert [i['card']['fixture_id'] for i in page['items']]==[fid]
            # A new save before the cursor does not duplicate/shift the next page.
            save(990001100)
            assert [i['card']['fixture_id'] for i in read_saved_page(db,user,cursor=first['next_cursor'])['items']]==[i['card']['fixture_id'] for i in second['items']]
        finally: tx.rollback()
