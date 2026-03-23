import requests
import json
import time
from kafka import KafkaProducer

# =========================
# CONFIGURATION
# =========================

API_TOKEN = "265c393d87814a4c8d5bc0d0c5124bfa"

API_URL = "https://api.football-data.org/v4/matches?status=FINISHED,IN_PLAY,PAUSED,SCHEDULED"
# API_URL = "https://api.football-data.org/v4/matches?dateFrom=2026-03-22&dateTo=2026-03-23"
BASE_URL = "https://api.football-data.org/v4"

headers = {
    "X-Auth-Token": API_TOKEN
}

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# =========================
# EXTRACTION
# =========================

# Fonction pour récupérer les matchs
def fetch_matches():
    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("matches", [])

    except requests.exceptions.Timeout:
        print("❌ Timeout : API trop lente")

    except requests.exceptions.ConnectionError:
        print("❌ Impossible de se connecter à l'API")

    except requests.exceptions.HTTPError as e:
        print("❌ Erreur HTTP :", e)

    except Exception as e:
        print("❌ Erreur inconnue :", e)

    return []


# Fonction pour récupérer les détails d’un match
def fetch_match_details(match_id):
    try:
        url = f"{BASE_URL}/matches/{match_id}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌[API ERROR] fetch_match_details match_id={match_id} :", e)
        return None

# =========================
# TRANSFORMATION (ETL)
# =========================

# Normalisation des données de match
def normalize_match(match):
    return {
        "match_id": match.get("id"),
        "competition": match.get("competition", {}).get("name"),
        "date_heure": match.get("utcDate"),
        "equipe_dom": match.get("homeTeam", {}).get("name"),
        "equipe_ext": match.get("awayTeam", {}).get("name"),
        "score_dom": match.get("score", {}).get("fullTime", {}).get("home"),
        "score_ext": match.get("score", {}).get("fullTime", {}).get("away"),
        "statut": match.get("status")
    }

# Normalisation des événements de but
def normalize_goal(match, goal):
    return {
        "event_type": "GOAL",
        "match_id": match.get("id"),
        "minute": goal.get("minute") or 0,
        "team": goal.get("team", {}).get("name"),
        "player_name": goal.get("scorer", {}).get("name") if goal.get("scorer") else None,
        "assist_name": goal.get("assist", {}).get("name") if goal.get("assist") else None,
        "goal_type": goal.get("type")
    }

# Normalisation des événements de carton
def normalize_card(match, card):
    return {
        "event_type": "CARD",
        "match_id": match.get("id"),
        "minute": card.get("minute") or 0,
        "team": card.get("team", {}).get("name"),
        "player_name": card.get("player", {}).get("name") if card.get("player") else None,
        "card": card.get("card")
    }

# =========================
# STREAMING KAFKA
# =========================


# Fonction principale de streaming vers Kafka
def stream_to_kafka():
    print(" Démarrage du producteur Kafka...")

    while True:
        matches = fetch_matches()

        for match in matches:
            try:
                match_data = normalize_match(match)

                if not match_data["match_id"]:
                    continue

                producer.send(
                    topic="live_matches",
                    key=str(match_data["match_id"]).encode(),
                    value=match_data
                )

                print(" Match envoyé :", match_data)

                details = fetch_match_details(match_data["match_id"])

                if details and "match" in details:
                    for g in details["match"].get("goals", []):
                        event = normalize_goal(match, g)
                        producer.send("match_events", value=event)

                    for c in details["match"].get("bookings", []):
                        event = normalize_card(match, c)
                        producer.send("match_events", value=event)

            except Exception as e:
                print(f"❌[PRODUCER ERROR] match_id={match.get('id')} :", e)

        producer.flush()
        time.sleep(60)

if __name__ == "__main__":
    stream_to_kafka()
