import json
import psycopg2
from kafka import KafkaConsumer

# =========================
# CONNEXION POSTGRES
# =========================

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

# =========================
# KAFKA CONSUMER
# =========================

consumer = KafkaConsumer(
    "live_matches",
    "match_events",
    bootstrap_servers="localhost:9092",
    group_id="football-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(" En attente des messages Kafka...")

# =========================
# VALIDATION
# =========================

# Fonctions de validation des données de match
def is_valid_match(data):
    required = ["match_id", "competition", "date_heure", "equipe_dom", "equipe_ext"]
    return all(data.get(k) is not None for k in required)

# Fonctions de validation des données d’événement
def is_valid_event(data):
    return data.get("match_id") is not None and data.get("minute") is not None

# =========================
# HISTORIQUE
# =========================

# Récupère l’état actuel d’un match
def get_current_match_state(cur, match_id):
    cur.execute("""
        SELECT score_dom, score_ext, statut
        FROM matchs_en_direct
        WHERE match_id = %s
    """, (match_id,))
    return cur.fetchone()

# Compare l’état actuel avec le nouveau pour détecter un changement
def has_match_changed(old, new):
    if old is None:
        return False

    old_score_dom, old_score_ext, old_statut = old

    return (
        old_score_dom != new["score_dom"]
        or old_score_ext != new["score_ext"]
        or old_statut != new["statut"]
    )

# Sauvegarde l’état précédent dans l’historique
def save_match_history(cur, data, old):
    old_score_dom, old_score_ext, old_statut = old

    cur.execute("""
        INSERT INTO matchs_historique (
            match_id, competition, date_heure,
            equipe_dom, equipe_ext,
            score_dom, score_ext, statut
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["match_id"],
        data["competition"],
        data["date_heure"],
        data["equipe_dom"],
        data["equipe_ext"],
        old_score_dom,
        old_score_ext,
        old_statut
    ))

# =========================
# CONSUMPTION
# =========================

# Traitement des messages Kafka
try:
    for message in consumer:
        data = message.value

        try:
            if message.topic == "live_matches":

                if not is_valid_match(data):
                    continue

                #  État actuel
                old_state = get_current_match_state(cur, data["match_id"])

                #  Historisation si changement
                if has_match_changed(old_state, data):
                    save_match_history(cur, data, old_state)
                    print(f"🕰️ Historique sauvegardé – match {data['match_id']}")

                #  Upsert principal
                cur.execute("""
                    INSERT INTO matchs_en_direct (
                        match_id, competition, date_heure,
                        equipe_dom, equipe_ext, score_dom,
                        score_ext, statut
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
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
                    data["match_id"],
                    data["competition"],
                    data["date_heure"],
                    data["equipe_dom"],
                    data["equipe_ext"],
                    data["score_dom"],
                    data["score_ext"],
                    data["statut"]
                ))

                print(" Match inséré / mis à jour")
                print(json.dumps(data, indent=2, ensure_ascii=False))

            elif message.topic == "match_events":

                if not is_valid_event(data):
                    continue

                if data["event_type"] == "GOAL":
                    cur.execute("""
                        INSERT INTO buteurs (
                            match_id, minute, team,
                            player_name, assist_name, goal_type
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        data["match_id"],
                        data["minute"],
                        data["team"],
                        data["player_name"],
                        data["assist_name"],
                        data["goal_type"]
                    ))

                elif data["event_type"] == "CARD":
                    cur.execute("""
                        INSERT INTO cartons (
                            match_id, minute, team,
                            player_name, card_type
                        ) VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        data["match_id"],
                        data["minute"],
                        data["team"],
                        data["player_name"],
                        data["card"]
                    ))

            conn.commit()

        except Exception as e:
            print(f"❌[DB ERROR] topic={message.topic} err={e}")
            conn.rollback()

except KeyboardInterrupt:
    print("\n🛑 Arrêt manuel")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print(" Connexions fermées")
