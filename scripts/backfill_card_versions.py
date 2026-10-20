"""Bootstrap presentation version history from retained complete match runs."""
from engine.eval.db import get_conn
from engine.eval.card_versions import DDL, publish

def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute('SELECT pg_advisory_xact_lock(734120281)')
            cur.execute('SELECT count(*) FROM raw.card_versions')
            if cur.fetchone()[0]:
                print('Version history already initialized; no historical replay performed.')
                return
            cur.execute("SELECT m.fixture_id,m.payload,m.run_ds,m.written_at FROM raw.match_analysis m JOIN raw.engine_runs r USING(run_ds) WHERE r.status='complete' ORDER BY m.written_at,m.run_ds,m.fixture_id")
            rows = cur.fetchall()
            for fid, payload, run, captured in rows:
                publish(cur, [(fid,payload)], run, captured)
            cur.execute('SELECT count(*) FROM raw.card_versions')
            print('Initialized card versions:', cur.fetchone()[0])

if __name__ == '__main__': main()
