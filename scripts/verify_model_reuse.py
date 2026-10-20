"""Compare cold, reused and forced model results against live inputs; no publication."""
import argparse
from datetime import datetime, timezone
from unittest.mock import patch
from engine.eval.db import get_conn
from engine.eval.model_cache import ModelCache, active_cache, digest
from engine.eval.run_model import RUNNERS, _run_bayesian

def main():
    p=argparse.ArgumentParser();p.add_argument('--model',choices=list(RUNNERS),required=True);p.add_argument('--days',type=int,default=7)
    args=p.parse_args();conn=get_conn()
    from engine.eval import predict_dixon_coles as dc
    class FixedClock:
        @staticmethod
        def now(tz=None): return frozen_time
    frozen_time=datetime.now(timezone.utc)
    clock=patch.object(dc, 'datetime', FixedClock)
    clock.start()
    try:
        cache=ModelCache(conn)
        runner=(lambda: _run_bayesian(args.days,include_analysis=True)) if args.model=='bayesian' else lambda: RUNNERS[args.model](args.days)
        with active_cache(cache):
            first=runner(); first_hash=digest(first)
            initial_misses=cache.misses
            warm=runner()
            if digest(warm)!=first_hash:
                print('COLD/WARM DIFFERENCE', str(first)[:1000], str(warm)[:1000], flush=True)
            assert digest(warm)==first_hash, 'Reused output differs from cold output'
            assert cache.hits>0, 'No computation was reused'
            assert cache.misses==initial_misses, 'Warm inputs changed; comparison needs a stable input window'
            warm_hits=cache.hits
            cache.force=True
            forced=runner()
            assert digest(forced)==first_hash, 'Reused output differs from forced full calculation'
        print({'model':args.model,'cold_computations':initial_misses,'warm_reuses':warm_hits,
               'equal_cold_warm_forced':True,'published':False})
    finally:
        conn.rollback();conn.close();clock.stop()

if __name__=='__main__':main()
