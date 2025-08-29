# airflow/dags/batch_etl.py
import os
import psycopg2
from datetime import datetime, timezone

PGHOST = os.getenv("PGHOST", "postgres")      # si Airflow est sur le même réseau Docker que Postgres
PGPORT = int(os.getenv("PGPORT", "5432"))     # port interne du conteneur Postgres
PGDB   = os.getenv("PGDATABASE", "dataflow360") # nom de la base
PGUSER = os.getenv("PGUSER", "postgres") # utilisateur Postgres 
PGPASS = os.getenv("PGPASSWORD", "nnbbvvv") # mot de passe utilisateur Postgres

# Création de la table d’agrégats si besoin
DDL_CREATE_AGG = """
CREATE TABLE IF NOT EXISTS agg_inplay_counts ( 
    id SERIAL PRIMARY KEY,
    run_time TIMESTAMPTZ NOT NULL,
    competition TEXT NOT NULL,
    inplay_count INT NOT NULL
);
"""

# Requête d’agrégation
SQL_AGG = """
SELECT competition, COUNT(*) AS inplay_count
FROM matchs_en_direct
WHERE statut = 'IN_PLAY'
GROUP BY competition
ORDER BY competition;
"""

# Requête d’insertion des résultats
SQL_INSERT = """
INSERT INTO agg_inplay_counts (run_time, competition, inplay_count)
VALUES (%s, %s, %s);
"""
 
# Fonction principale pour exécuter le batch ETL
def run():
    conn = None
    try:
        conn = psycopg2.connect(
            host=PGHOST, port=PGPORT, database=PGDB, user=PGUSER, password=PGPASS
        )
        cur = conn.cursor()

        # 1) table d’agrégats
        cur.execute(DDL_CREATE_AGG)
        conn.commit()

        # 2) calcul de l’agrégat courant
        cur.execute(SQL_AGG)
        rows = cur.fetchall()

        run_time = datetime.now(timezone.utc)

        # 3) insertion des résultats
        for competition, inplay_count in rows:
            cur.execute(SQL_INSERT, (run_time, competition, inplay_count))

        conn.commit()

        print(f"[batch_etl] OK - {len(rows)} lignes insérées dans agg_inplay_counts @ {run_time.isoformat()}")
        if not rows:
            print("[batch_etl] Aucun match IN_PLAY au moment du run.")

        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        print("[batch_etl] ERREUR:", e)
        raise
    finally:
        if conn:
            conn.close()

# Permet d'exécuter localement si besoin: python3 airflow/dags/batch_etl.py
if __name__ == "__main__":
    run()
