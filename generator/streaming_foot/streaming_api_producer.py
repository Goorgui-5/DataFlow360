import requests
import json
import time
from kafka import KafkaProducer

# Clé API Football-Data.org
API_TOKEN = "98080fa6a73e4cc2a5b700b4ec5e30ca"

# URL API
# API_URL = "https://api.football-data.org/v4/matches?status=SCHEDULED,IN_PLAY,FINISHED" 
API_URL = "https://api.football-data.org/v4/matches?dateFrom=2025-11-25&dateTo=2025-11-26"
# URL pour les détails des matchs
BASE_URL = "https://api.football-data.org/v4"

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Headers API
headers = {
    "X-Auth-Token": API_TOKEN
}

# Fonction pour récupérer les matchs
def fetch_matches():
    """Récupère la liste des matchs du jour."""
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("matches", [])
    except Exception as e:
        print("❌ Erreur API matches :", e)
        return []

# Fonction pour récupérer les détails d’un match
def fetch_match_details(match_id):
    """Récupère les détails d’un match (buts, cartons...)."""
    try:
        url = f"{BASE_URL}/matches/{match_id}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Erreur API détails pour match {match_id} :", e)
        return None

# Fonction principale de streaming vers Kafka
def stream_to_kafka():
    print(" Démarrage du producteur Kafka pour les matchs live...")
    while True:
        matches = fetch_matches()
        for match in matches:
            try:
                data = {
                    "match_id": match.get("id"),
                    "competition": match.get("competition", {}).get("name"),
                    "date_heure": match.get("utcDate"),
                    "equipe_dom": match.get("homeTeam", {}).get("name"),
                    "equipe_ext": match.get("awayTeam", {}).get("name"),
                    "score_dom": match.get("score", {}).get("fullTime", {}).get("home"),
                    "score_ext": match.get("score", {}).get("fullTime", {}).get("away"),
                    "statut": match.get("status")
                }
                print(" Match envoyé :", data)
                producer.send("live_matches", value=data)

                # Récupération des événements du match
                details = fetch_match_details(match.get("id"))
                if details and "match" in details:
                    # Buts
                    for g in details["match"].get("goals", []):
                        event = {
                            "event_type": "GOAL",
                            "match_id": match.get("id"),
                            "competition": match.get("competition", {}).get("name"),
                            "date_heure": match.get("utcDate"),
                            "minute": g.get("minute"),
                            "team": g.get("team", {}).get("name"),
                            "player": g.get("scorer"),
                            "assist": g.get("assist"),
                            "goal_type": g.get("type"),
                            "score": g.get("score")
                        }
                        print(" Événement BUT :", event)
                        producer.send("match_events", value=event)

                    # Cartons
                    for c in details["match"].get("bookings", []):
                        event = {
                            "event_type": "CARD",
                            "match_id": match.get("id"),
                            "minute": c.get("minute"),
                            "team": c.get("team", {}).get("name"),
                            "player": c.get("player"),
                            "card": c.get("card")
                        }
                        print(" Événement CARTON :", event)
                        producer.send("match_events", value=event)

            except Exception as e:
                print(f"❌ Erreur traitement match {match.get('id')} :", e)

        producer.flush()
        time.sleep(60)

if __name__ == "__main__":
    stream_to_kafka()
