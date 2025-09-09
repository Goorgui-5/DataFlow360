# loader.py
import psycopg2
from datetime import datetime

# Connexion Postgres
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

# 1) Extraire depuis la base source
cur.execute("SELECT id, competition, equipe_dom, equipe_ext, date_match, buts_dom, buts_ext, statut FROM matchs_en_direct;")
rows = cur.fetchall()

for r in rows:
    _, competition, equipe_dom, equipe_ext, date_match, buts_dom, buts_ext, statut = r

    # 2) Insérer dimension compétition
    cur.execute("INSERT INTO dim_competition (competition_name) VALUES (%s) ON CONFLICT (competition_name) DO NOTHING RETURNING id_competition;", (competition,))
    comp_id = cur.fetchone()[0] if cur.rowcount > 0 else None
    if not comp_id:
        cur.execute("SELECT id_competition FROM dim_competition WHERE competition_name=%s;", (competition,))
        comp_id = cur.fetchone()[0]

    # 3) Insérer équipes
    for team, table in [(equipe_dom, "id_home_team"), (equipe_ext, "id_away_team")]:
        cur.execute("INSERT INTO dim_team (team_name) VALUES (%s) ON CONFLICT (team_name) DO NOTHING RETURNING id_team;", (team,))
        team_id = cur.fetchone()[0] if cur.rowcount > 0 else None
        if not team_id:
            cur.execute("SELECT id_team FROM dim_team WHERE team_name=%s;", (team,))
            team_id = cur.fetchone()[0]
        if table == "id_home_team":
            home_id = team_id
        else:
            away_id = team_id

    # 4) Insérer dimension temps
    date_obj = date_match.date() if isinstance(date_match, datetime) else date_match
    cur.execute("""INSERT INTO dim_time (match_date, year, month, day) 
                   VALUES (%s, %s, %s, %s) 
                   ON CONFLICT (match_date) DO NOTHING RETURNING id_time;""",
                (date_obj, date_obj.year, date_obj.month, date_obj.day))
    time_id = cur.fetchone()[0] if cur.rowcount > 0 else None
    if not time_id:
        cur.execute("SELECT id_time FROM dim_time WHERE match_date=%s;", (date_obj,))
        time_id = cur.fetchone()[0]

    # 5) Insérer dans la table de faits
    cur.execute("""INSERT INTO fact_match 
                   (id_competition, id_home_team, id_away_team, id_time, goals_home, goals_away, statut) 
                   VALUES (%s,%s,%s,%s,%s,%s,%s);""",
                (comp_id, home_id, away_id, time_id, buts_dom, buts_ext, statut))

conn.commit()
cur.close()
conn.close()
print("✅ Chargement terminé")


# import psycopg2
# import pandas as pd

# # Connexion à PostgreSQL
# conn = psycopg2.connect(
#     host="localhost",
#     port=5433,
#     database="dataflow360",
#     user="postgres",
#     password="nnbbvv"
# )
# cur = conn.cursor()

# # Charger le CSV
# df = pd.read_csv("/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/batch_foot/foot_fin.csv")

# # Remplacer les NaN par None (pour éviter erreurs SQL)
# df = df.where(pd.notnull(df), None)

# # Insertion ligne par ligne
# for i, row in df.iterrows():
#     cur.execute("""
#         INSERT INTO saisons (
#             saison, equipe, pays, competition, classement,
#             matchs_joues, victoires, nuls, defaites,
#             buts_pour, buts_contre, difference_buts,
#             points, meilleur_buteur,
#             gardien_de_but
#         )
#         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
#     """, (
#         row.saison, row.equipe, row.pays, row.competition, row.classement,
#         row.matchs_joues, row.victoires, row.nuls, row.defaites,
#         row.buts_pour, row.buts_contre, row.difference_buts,
#         row.points, row.meilleur_buteur,
#         row.gardien_de_but
#     ))

# conn.commit()
# cur.close()
# conn.close()

# print("Données insérées avec succès dans PostgreSQL")
