import io
import os
import uuid
import pytest
import psycopg2
from psycopg2 import sql
from engine.eval.db import get_conn
from engine.eval.forecast_archive import archive

class Store:
    def __init__(self): self.objects={};self.corrupt=False
    def put_object(self, Bucket, Key, Body, **kw): self.objects[Key]=Body
    def get_object(self, Bucket, Key): return {'Body':io.BytesIO(b'bad' if self.corrupt else self.objects[Key])}

@pytest.mark.skipif(os.getenv('TEST_FORECAST_ARCHIVE')!='1',reason='Requires disposable database creation')
def test_full_incremental_and_failed_verification():
    admin=get_conn();admin.autocommit=True
    name='archive_test_'+uuid.uuid4().hex[:12]
    with admin.cursor() as cur:cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    params=admin.get_dsn_parameters();params['dbname']=name;params['password']=os.getenv('DB_PASSWORD')
    conn=psycopg2.connect(**params)
    try:
        with conn,conn.cursor() as cur:
            cur.execute('CREATE SCHEMA forecast')
            for table in ('snapshots','entries','settlements'):
                cur.execute(f'CREATE TABLE forecast.{table}(id bigserial PRIMARY KEY, data jsonb)')
                cur.execute(f'''INSERT INTO forecast.{table}(data) VALUES ('{{"test":1}}'),('{{"test":2}}')''')
        s3=Store();s3.corrupt=True
        with pytest.raises(ValueError):archive(conn,s3,'test',1,'full')
        with conn,conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM forecast_archive.checkpoints');assert cur.fetchone()[0]==0
        s3.corrupt=False
        assert archive(conn,s3,'test',1,'full')['copied_rows']==dict.fromkeys(('snapshots','entries','settlements'),2)
        count=len(s3.objects)
        assert not any(archive(conn,s3,'test')['copied_rows'].values())
        assert len(s3.objects)==count
        with conn,conn.cursor() as cur:
            cur.execute('''INSERT INTO forecast.settlements(data) VALUES ('{"correction":"miss"}')''')
        assert archive(conn,s3,'test')['copied_rows']=={'snapshots':0,'entries':0,'settlements':1}
        with conn,conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM forecast.settlements');assert cur.fetchone()[0]==3
    finally:
        conn.close()
        with admin.cursor() as cur:cur.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
        admin.close()
