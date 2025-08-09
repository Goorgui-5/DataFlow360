# generator/streaming_api_producer.py

import requests
import json
import time
from kafka import KafkaProducer

# Ta clé API Football-Data.org
API_TOKEN = "98080fa6a73e4cc2a5b700b4ec5e30ca"

# URL de l’API pour les matchs du jour
API_URL = "https://api.football-data.org/v4/matches"

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Headers pour authentifier la requête API
headers = {
    "X-Auth-Token": API_TOKEN
}

def fetch_matches():
    try:
        response = requests.get(API_URL, headers=headers)
        data = response.json()
        matches = data.get("matches", [])
        return matches
    except Exception as e:
        print("Erreur API:", e)
        return []

def stream_to_kafka():
    print("Démarrage du producteur Kafka pour les matchs live...")
    while True:
        matches = fetch_matches()
        for match in matches:
            data = {
                "match_id": match["id"],
                "competition": match["competition"]["name"],
                "date_heure": match["utcDate"],
                "equipe_dom": match["homeTeam"]["name"],
                "equipe_ext": match["awayTeam"]["name"],
                "score_dom": match["score"]["fullTime"]["home"],
                "score_ext": match["score"]["fullTime"]["away"],
                "statut": match["status"]
            }
            print("Match envoyé :", data)
            producer.send("live_matches", value=data)
        time.sleep(60)  # toutes les 15 secondes

if __name__ == "__main__":
    stream_to_kafka()
