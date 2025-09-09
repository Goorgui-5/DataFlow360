# airflow/dags/batch_etl.py
import os
import socket
import psycopg2
from datetime import datetime, timezone

def resolve_pg_host():
    """
    Détermine automatiquement si on doit utiliser 'localhost'
    ou 'host.docker.internal'
    """
    default_host = os.getenv("PGHOST", "host.docker.internal")

    try:
        # test si le host est résolvable
        socket.gethostbyname(default_host)
        return default_host
    except socket.error:
        # fallback → localhost
        return "localhost"

PGHOST = resolve_pg_host()
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDATABASE", "dataflow360")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASS = os.getenv("PGPASSWORD", "nnbbvv")

DDL_CREATE_AGG = """
CREATE TABLE IF NOT EXISTS agg_inplay_counts ( 
    id SERIAL PRIMARY KEY,
    run_time TIMESTAMPTZ NOT NULL,
    competition TEXT NOT NULL,
    inplay_count INT NOT NULL
);
"""

SQL_AGG = """
SELECT competition, COUNT(*) AS inplay_count
FROM matchs_en_direct
WHERE statut = 'IN_PLAY'
GROUP BY competition
ORDER BY competition;
"""

SQL_INSERT = """
INSERT INTO agg_inplay_counts (run_time, competition, inplay_count)
VALUES (%s, %s, %s);
"""

def run():
    conn = None
    try:
        conn = psycopg2.connect(
            host=PGHOST, port=PGPORT, database=PGDB, user=PGUSER, password=PGPASS
        )
        cur = conn.cursor()

        cur.execute(DDL_CREATE_AGG)
        conn.commit()

        cur.execute(SQL_AGG)
        rows = cur.fetchall()

        run_time = datetime.now(timezone.utc)

        for competition, inplay_count in rows:
            cur.execute(SQL_INSERT, (run_time, competition, inplay_count))

        conn.commit()

        print(f"[batch_etl] {len(rows)} lignes insérées dans agg_inplay_counts @ {run_time.isoformat()}")
        if not rows:
            print("[batch_etl] Aucun match IN_PLAY au moment du run.")

        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        print("[batch_etl] ❌ ERREUR:", e)
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run()
