import psycopg2
import pandas as pd

# Connexion à PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

# Charger le CSV
df = pd.read_csv("/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/batch_foot/foot_fin.csv")

# Remplacer les NaN par None (pour éviter erreurs SQL)
df = df.where(pd.notnull(df), None)

# Insertion ligne par ligne
for i, row in df.iterrows():
    cur.execute("""
        INSERT INTO saisons (
            saison, equipe, pays, competition, classement,
            matchs_joues, victoires, nuls, defaites,
            buts_pour, buts_contre, difference_buts,
            points, meilleur_buteur,
            gardien_de_but
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """, (
        row.saison, row.equipe, row.pays, row.competition, row.classement,
        row.matchs_joues, row.victoires, row.nuls, row.defaites,
        row.buts_pour, row.buts_contre, row.difference_buts,
        row.points, row.meilleur_buteur,
        row.gardien_de_but
    ))

conn.commit()
cur.close()
conn.close()

print("Données insérées avec succès dans PostgreSQL")
