"""Run the read-only research API on loopback using an explicit connection source.

Use FORECAST_API_DSN for a dedicated reader, or pass --project-database to use
this local project's existing database configuration for development checks.
Never prints the connection string. Does not create users or grant permissions.
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-database', action='store_true')
    parser.add_argument('--port', type=int, default=8001)
    args = parser.parse_args()
    if args.project_database:
        from dotenv import load_dotenv
        from psycopg2.extensions import make_dsn
        load_dotenv(ROOT / '.env')
        required = ('DB_NAME', 'DB_USER', 'DB_PASSWORD')
        if any(not os.getenv(k) for k in required):
            parser.error('The local database configuration is incomplete.')
        os.environ['FORECAST_API_DSN'] = make_dsn(
            dbname=os.environ['DB_NAME'], user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'], host='127.0.0.1',
            port=os.getenv('DB_PORT', '5432'),
            options='-c default_transaction_read_only=on')
    elif not os.getenv('FORECAST_API_DSN'):
        parser.error('Set FORECAST_API_DSN or explicitly choose --project-database.')
    import uvicorn
    uvicorn.run('engine.api.app:app', host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
