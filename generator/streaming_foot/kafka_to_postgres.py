import json
import psycopg2
from kafka import KafkaConsumer

# Connexion à PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

# Configuration du consumer Kafka
consumer = KafkaConsumer(
    'live_matches',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='matchs-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(" En attente des messages Kafka...")

try:
    for message in consumer:
        match = message.value
        print(f"\n Match reçu : {json.dumps(match, indent=2)}")

        try:
            cur.execute("""
                INSERT INTO matchs_en_direct (
                    match_id, competition, date_heure,
                    equipe_dom, equipe_ext, score_dom,
                    score_ext, statut
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_id) DO UPDATE SET
                    competition = EXCLUDED.competition,
                    date_heure = EXCLUDED.date_heure,
                    equipe_dom = EXCLUDED.equipe_dom,
                    equipe_ext = EXCLUDED.equipe_ext,
                    score_dom = EXCLUDED.score_dom,
                    score_ext = EXCLUDED.score_ext,
                    statut = EXCLUDED.statut,
                    last_updated = CURRENT_TIMESTAMP;
            """, (
                match["match_id"],
                match["competition"],
                match["date_heure"],
                match["equipe_dom"],
                match["equipe_ext"],
                match["score_dom"],
                match["score_ext"],
                match["statut"]
            ))
            conn.commit()
            print(" Match inséré/mis à jour avec succès.")
        except Exception as e:
            print("❌ Erreur d'insertion :", e)
            conn.rollback()

except KeyboardInterrupt:
    print("\n Interruption manuelle. Fermeture propre...")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print(" Connexions fermées.")
