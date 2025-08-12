import json
import psycopg2
from kafka import KafkaConsumer

# Connexion PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

# Consumer Kafka : écoute 2 topics
consumer = KafkaConsumer(
    'live_matches',
    'match_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='football-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(" En attente des messages Kafka...")

try:
    for message in consumer:
        data = message.value

        try:
            # Détection du type de message
            if message.topic == "live_matches":
                print(f"Match reçu : {json.dumps(data, indent=2)}")

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
                    data.get("match_id"),
                    data.get("competition"),
                    data.get("date_heure"),
                    data.get("equipe_dom"),
                    data.get("equipe_ext"),
                    data.get("score_dom"),
                    data.get("score_ext"),
                    data.get("statut")
                ))
                print("Match inséré/mis à jour.")

            elif message.topic == "match_events":
                print(f"Événement reçu : {json.dumps(data, indent=2)}")

                if data.get("event_type") == "GOAL":
                    cur.execute("""
                        INSERT INTO buteurs (
                            match_id, minute, team, player_name, assist_name, goal_type
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        data.get("match_id"),
                        data.get("minute"),
                        data.get("team"),
                        data.get("player", {}).get("name") if data.get("player") else None,
                        data.get("assist", {}).get("name") if data.get("assist") else None,
                        data.get("goal_type")
                    ))
                    print("Buteur inséré.")

                elif data.get("event_type") == "CARD":
                    cur.execute("""
                        INSERT INTO cartons (
                            match_id, minute, team, player_name, card_type
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        data.get("match_id"),
                        data.get("minute"),
                        data.get("team"),
                        data.get("player", {}).get("name") if data.get("player") else None,
                        data.get("card")
                    ))
                    print("Carton inséré.")

            # Commit après chaque insertion
            conn.commit()

        except Exception as e:
            print("❌ Erreur d'insertion :", e)
            conn.rollback()

except KeyboardInterrupt:
    print("\n🛑 Interruption manuelle. Fermeture propre...")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Connexions fermées.")
