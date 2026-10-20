"""Dependency-keyed computation reuse. Off outside the model runner by default."""
from contextvars import ContextVar
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
import hashlib
import json
import math
import sys
from importlib.metadata import version
import numpy as np
import pandas as pd
from psycopg2.extras import Json

_ACTIVE = ContextVar('model_cache', default=None)
DDL = '''CREATE TABLE IF NOT EXISTS raw.model_result_versions (
 id bigserial PRIMARY KEY, scope text NOT NULL, entity_key text NOT NULL,
 output_hash text NOT NULL, output jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS raw.model_processing_state (
 scope text NOT NULL, entity_key text NOT NULL, input_hash text NOT NULL,
 result_id bigint NOT NULL REFERENCES raw.model_result_versions(id),
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(scope,entity_key));'''


def pack(value):
    if value is None or value is pd.NA or value is pd.NaT: return None
    if isinstance(value, np.generic): return pack(value.item())
    if isinstance(value, datetime): return {'$time':value.isoformat()}
    if isinstance(value, date): return {'$date':value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value): return {'$float':str(value)}
    if isinstance(value, dict):
        items=[[pack(k),pack(v)] for k,v in value.items()]
        return {'$dict':sorted(items,key=lambda x:json.dumps(x[0],sort_keys=True))}
    if isinstance(value, tuple): return {'$tuple':[pack(x) for x in value]}
    if isinstance(value, list): return [pack(x) for x in value]
    if isinstance(value,pd.DataFrame): return pack({'columns':list(value.columns),'rows':value.to_dict('records')})
    if isinstance(value,pd.Series): return pack(value.to_dict())
    return value


def unpack(value):
    if isinstance(value,list): return [unpack(x) for x in value]
    if isinstance(value,dict):
        if '$dict' in value: return {unpack(k):unpack(v) for k,v in value['$dict']}
        if '$tuple' in value: return tuple(unpack(x) for x in value['$tuple'])
        if '$time' in value: return datetime.fromisoformat(value['$time'])
        if '$date' in value: return date.fromisoformat(value['$date'])
        if '$float' in value: return float(value['$float'])
    return value


def digest(value):
    return hashlib.sha256(json.dumps(pack(value),sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def implementation_hash():
    # Conservative invalidation for any engine/config change, including helpers.
    root=Path(__file__).parents[1]
    paths=sorted(root.rglob('*.py'))+[root/'eval/calibration_maps.json']
    return digest({'source': [(str(p.relative_to(root)),hashlib.sha256(p.read_bytes()).hexdigest()) for p in paths if p.exists()],
                   'runtime': [sys.version, version('numpy'), version('pandas'), version('scipy')]})


class ModelCache:
    def __init__(self,conn,force=False):
        self.conn=conn;self.force=force;self.version=implementation_hash()
        self.hits=0;self.misses=0;self.new_versions=0
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(734120269)')
            cur.execute(DDL)
        conn.commit()

    def memo(self,scope,key,inputs,compute,encode=lambda x:x,decode=lambda x:x):
        key=str(key); fingerprint=digest({'implementation':self.version,'inputs':inputs})
        # Pending versions and state commit only once the entire model succeeds.
        with self.conn.cursor() as cur:
            cur.execute('''SELECT s.input_hash,v.id,v.output_hash,v.output FROM raw.model_processing_state s
                JOIN raw.model_result_versions v ON v.id=s.result_id WHERE s.scope=%s AND s.entity_key=%s''',(scope,key))
            old=cur.fetchone()
            if old and old[0]==fingerprint and not self.force:
                self.hits+=1;return decode(unpack(old[3]))
            result=compute()  # Exceptions never advance state.
            data=encode(result); output_hash=digest(data); self.misses+=1
            if old and old[2]==output_hash:
                result_id=old[1]
            else:
                cur.execute('''INSERT INTO raw.model_result_versions(scope,entity_key,output_hash,output)
                    VALUES (%s,%s,%s,%s) RETURNING id''',(scope,key,output_hash,Json(pack(data))))
                result_id=cur.fetchone()[0];self.new_versions+=1
            cur.execute('''INSERT INTO raw.model_processing_state(scope,entity_key,input_hash,result_id)
                VALUES (%s,%s,%s,%s) ON CONFLICT(scope,entity_key) DO UPDATE
                SET input_hash=EXCLUDED.input_hash,result_id=EXCLUDED.result_id,processed_at=clock_timestamp()''',
                (scope,key,fingerprint,result_id))
            return result


@contextmanager
def active_cache(cache):
    token=_ACTIVE.set(cache)
    try:yield cache
    finally:_ACTIVE.reset(token)


def cached(scope,key,inputs,compute,encode=lambda x:x,decode=lambda x:x):
    cache=_ACTIVE.get()
    return compute() if cache is None else cache.memo(scope,key,inputs,compute,encode,decode)
