import requests
import json
import time
from kafka import KafkaProducer

# Clé API Football-Data.org
API_TOKEN = "98080fa6a73e4cc2a5b700b4ec5e30ca"

# URL API
API_URL = "https://api.football-data.org/v4/matches?status" 
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


# # generator/streaming_api_producer.py

# import requests
# import json
# import time
# from kafka import KafkaProducer

# # Ta clé API Football-Data.org
# API_TOKEN = "98080fa6a73e4cc2a5b700b4ec5e30ca"

# # URL de l’API pour les matchs du jour
# API_URL = "https://api.football-data.org/v4/matches"

# # URL de l'API pour les actions en direct
# BASE_URL = "https://api.football-data.org/v4"

# # Kafka configuration
# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # Headers pour authentifier la requête API
# headers = {
#     "X-Auth-Token": API_TOKEN
# }

# def fetch_matches():
#     try:
#         response = requests.get(API_URL, headers=headers)
#         data = response.json()
#         matches = data.get("matches", [])
#         return matches
#     except Exception as e:
#         print("Erreur API:", e)
#         return []

# def fetch_match_details(match_id):
#     # Récupère les détails d’un match (buts, cartons...)
#     try:
#         url = f"{BASE_URL}/matches/{match_id}"
#         r = requests.get(url, headers=headers)
#         if r.status_code == 200:
#             return r.json()
#     except Exception as e:
#         print("Erreur détails match:", e)
#     return None

# def stream_to_kafka():
#     print("Démarrage du producteur Kafka pour les matchs live...")
#     while True:
#         matches = fetch_matches()
#         for match in matches:
#             # Envoi du résumé du match
#             data = {
#                 "match_id": match["id"],
#                 "competition": match["competition"]["name"],
#                 "date_heure": match["utcDate"],
#                 "equipe_dom": match["homeTeam"]["name"],
#                 "equipe_ext": match["awayTeam"]["name"],
#                 "score_dom": match["score"]["fullTime"]["home"],
#                 "score_ext": match["score"]["fullTime"]["away"],
#                 "statut": match["status"]
#             }
#             print("Match envoyé :", data)
#             producer.send("live_matches", value=data)

#             # Ajout : récupération des événements
#             details = fetch_match_details(match["id"])
#             if details and "match" in details:
#                 # pour les buts
#                 goals = details["match"].get("goals", [])
#                 for g in goals:
#                     event = {
#                         "event_type": "GOAL",
#                         "match_id": match["id"],
#                         "competition": match["competition"]["name"],
#                         "date_heure": match["utcDate"],
#                         "minute": g.get("minute"),
#                         "team": g.get("team", {}).get("name"),
#                         "player": g.get("scorer"),
#                         "assist": g.get("assist"),
#                         "goal_type": g.get("type"),
#                         "score": g.get("score")
#                     }
#                     producer.send("match_events", value=event)
#                 # pour les cartons
#                 cards = details["match"].get("bookings", [])
#                 for c in cards:
#                     event = {
#                         "event_type": "CARD",
#                         "match_id": match["id"],
#                         "minute": c.get("minute"),
#                         "team": c.get("team", {}).get("name"),
#                         "player": c.get("player"),
#                         "card": c.get("card")
#                     }
#                     producer.send("match_events", value=event)

#         producer.flush()
#         time.sleep(60)

# if __name__ == "__main__":
#     stream_to_kafka()