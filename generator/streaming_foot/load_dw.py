import psycopg2
import logging
from datetime import datetime

# ============================================================
#  load_dw.py — Chargement automatique du Data Warehouse
# ============================================================
#
#  Ce script lit les tables public (matchs_en_direct, cartons)
#  et alimente automatiquement le schéma dw :
#    - dw.dim_equipe
#    - dw.dim_competition
#    - dw.dim_date
#    - dw.fait_match
#
#  À lancer après chaque cycle Kafka, ou via Airflow.
# ============================================================

# ── CONFIGURATION ─────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "dataflow360",
    "user":     "postgres",
    "password": "nnbbvv"
}

# ── LOGS ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("load_dw")


# ============================================================
# CONNEXION
# ============================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# ÉTAPE 1 — Charger dw.dim_competition
# ============================================================

def load_dim_competition(cur):
    """
    Insère dans dw.dim_competition toutes les compétitions
    présentes dans matchs_en_direct qui n'existent pas encore.
    """
    cur.execute("""
        INSERT INTO dw.dim_competition (nom_competition)
        SELECT DISTINCT competition
        FROM matchs_en_direct
        WHERE competition IS NOT NULL
        ON CONFLICT (nom_competition) DO NOTHING
    """)
    log.info(f"  dim_competition : {cur.rowcount} nouvelles lignes insérées")


# ============================================================
# ÉTAPE 2 — Charger dw.dim_equipe
# ============================================================

def load_dim_equipe(cur):
    """
    Insère dans dw.dim_equipe toutes les équipes
    (domicile + extérieur) présentes dans matchs_en_direct.
    """
    cur.execute("""
        INSERT INTO dw.dim_equipe (nom_equipe)
        SELECT DISTINCT equipe_nom
        FROM (
            SELECT equipe_dom AS equipe_nom FROM matchs_en_direct
            UNION
            SELECT equipe_ext AS equipe_nom FROM matchs_en_direct
        ) AS equipes
        WHERE equipe_nom IS NOT NULL
        ON CONFLICT (nom_equipe) DO NOTHING
    """)
    log.info(f"  dim_equipe : {cur.rowcount} nouvelles lignes insérées")


# ============================================================
# ÉTAPE 3 — Charger dw.dim_date
# ============================================================

def load_dim_date(cur):
    """
    Insère dans dw.dim_date toutes les dates distinctes
    présentes dans matchs_en_direct.
    """
    cur.execute("""
        INSERT INTO dw.dim_date (date_complete, jour, mois, annee, trimestre)
        SELECT DISTINCT
            DATE(date_heure)                    AS date_complete,
            EXTRACT(DAY     FROM date_heure)    AS jour,
            EXTRACT(MONTH   FROM date_heure)    AS mois,
            EXTRACT(YEAR    FROM date_heure)    AS annee,
            EXTRACT(QUARTER FROM date_heure)    AS trimestre
        FROM matchs_en_direct
        WHERE date_heure IS NOT NULL
        ON CONFLICT (date_complete) DO NOTHING
    """)
    log.info(f"  dim_date : {cur.rowcount} nouvelles lignes insérées")


# ============================================================
# ÉTAPE 4 — Charger dw.fait_match
# ============================================================

def load_fait_match(cur):
    """
    Insère dans dw.fait_match les matchs TERMINÉS (statut FINISHED)
    qui ne sont pas encore présents dans le DW.

    On filtre sur FINISHED pour ne stocker que des données finales.
    Les scores null sont ignorés.
    """
    cur.execute("""
        INSERT INTO dw.fait_match (
            match_id,
            id_date,
            id_equipe_dom,
            id_equipe_ext,
            id_competition,
            score_dom,
            score_ext,
            nb_buts_total,
            nb_cartons_total
        )
        SELECT
            m.match_id,
            d.id_date,
            edom.id_equipe,
            eext.id_equipe,
            c.id_competition,
            m.score_dom,
            m.score_ext,
            COALESCE(m.score_dom, 0) + COALESCE(m.score_ext, 0),
            (
                SELECT COUNT(*)
                FROM cartons ca
                WHERE ca.match_id = m.match_id
            )
        FROM matchs_en_direct m

        -- Jointure dimension date
        JOIN dw.dim_date d
            ON DATE(m.date_heure) = d.date_complete

        -- Jointure équipe domicile
        JOIN dw.dim_equipe edom
            ON m.equipe_dom = edom.nom_equipe

        -- Jointure équipe extérieure
        JOIN dw.dim_equipe eext
            ON m.equipe_ext = eext.nom_equipe

        -- Jointure compétition
        JOIN dw.dim_competition c
            ON m.competition = c.nom_competition

        WHERE
            m.match_id   IS NOT NULL
            AND m.score_dom IS NOT NULL
            AND m.score_ext IS NOT NULL
            AND m.statut = 'FINISHED'      -- seulement les matchs terminés

        ON CONFLICT DO NOTHING
    """)
    log.info(f"  fait_match : {cur.rowcount} nouvelles lignes insérées")


# ============================================================
# ÉTAPE 5 — Vérifications de cohérence (optionnel)
# ============================================================

def run_quality_checks(cur):
    """
    Vérifie la cohérence du DW après chargement.
    Logue un warning si un problème est détecté.
    """
    checks = {
        "Faits sans équipe dom": """
            SELECT COUNT(*) FROM dw.fait_match f
            LEFT JOIN dw.dim_equipe e ON f.id_equipe_dom = e.id_equipe
            WHERE e.id_equipe IS NULL
        """,
        "Faits sans équipe ext": """
            SELECT COUNT(*) FROM dw.fait_match f
            LEFT JOIN dw.dim_equipe e ON f.id_equipe_ext = e.id_equipe
            WHERE e.id_equipe IS NULL
        """,
        "Faits sans compétition": """
            SELECT COUNT(*) FROM dw.fait_match f
            LEFT JOIN dw.dim_competition c ON f.id_competition = c.id_competition
            WHERE c.id_competition IS NULL
        """,
        "Faits sans date": """
            SELECT COUNT(*) FROM dw.fait_match f
            LEFT JOIN dw.dim_date d ON f.id_date = d.id_date
            WHERE d.id_date IS NULL
        """,
        "Buts incohérents": """
            SELECT COUNT(*) FROM dw.fait_match
            WHERE nb_buts_total != COALESCE(score_dom,0) + COALESCE(score_ext,0)
        """,
    }

    all_ok = True
    for nom, sql in checks.items():
        cur.execute(sql)
        n = cur.fetchone()[0]
        if n > 0:
            log.warning(f"  ⚠️  {nom} : {n} ligne(s) problématique(s)")
            all_ok = False
        else:
            log.info(f"  ✅  {nom} : OK")

    return all_ok


# ============================================================
# ÉTAPE 6 — Stats finales
# ============================================================

def log_stats(cur):
    """Affiche un résumé des volumes dans le DW."""
    stats = {
        "dim_equipe":     "SELECT COUNT(*) FROM dw.dim_equipe",
        "dim_competition":"SELECT COUNT(*) FROM dw.dim_competition",
        "dim_date":       "SELECT COUNT(*) FROM dw.dim_date",
        "fait_match":     "SELECT COUNT(*) FROM dw.fait_match",
    }
    log.info("── Volumes DW ──────────────────────────")
    for table, sql in stats.items():
        cur.execute(sql)
        n = cur.fetchone()[0]
        log.info(f"  {table:<20} : {n} lignes")
    log.info("────────────────────────────────────────")


# ============================================================
# POINT D'ENTRÉE PRINCIPAL
# ============================================================

def run():
    """
    Lance le chargement complet du DW.
    Appelé directement ou depuis Airflow.
    """
    start = datetime.now()
    log.info("=" * 50)
    log.info("Démarrage du chargement DW")
    log.info("=" * 50)

    conn = None
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # ── Chargement des dimensions ──────────────
        log.info("── Chargement des dimensions ────────────")
        load_dim_competition(cur)
        load_dim_equipe(cur)
        load_dim_date(cur)

        # ── Chargement de la table de faits ────────
        log.info("── Chargement de la table de faits ──────")
        load_fait_match(cur)

        # ── Commit ─────────────────────────────────
        conn.commit()
        log.info("✅ Commit effectué avec succès")

        # ── Vérifications de qualité ───────────────
        log.info("── Vérifications de qualité ─────────────")
        ok = run_quality_checks(cur)

        # ── Statistiques finales ───────────────────
        log_stats(cur)

        duree = (datetime.now() - start).seconds
        log.info(f"✅ Chargement DW terminé en {duree}s — {'OK' if ok else 'AVEC WARNINGS'}")

        return True

    except Exception as e:
        log.error(f"❌ Erreur lors du chargement DW : {e}")
        if conn:
            conn.rollback()
            log.info("Rollback effectué")
        raise  # Airflow détecte l'échec si on relance l'exception

    finally:
        if conn:
            cur.close() 
            conn.close()
            log.info("Connexion fermée")


# ── Lancement direct ──────────────────────────────────────
if __name__ == "__main__":
    run()
