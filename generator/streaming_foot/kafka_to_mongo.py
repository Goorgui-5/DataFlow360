# consumers/kafka_to_mongo.py
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Connexion MongoDB (Docker)
client = MongoClient("mongodb://root:example@localhost:27017/")
db = client["dataflow360"]

# Collections
col_matches = db["matchs_live"]
col_events = db["match_events"]

# Kafka Consumer
consumer = KafkaConsumer(
    'live_matches',
    'match_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='mongo-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("En attente des messages Kafka (MongoDB)...")

# Traitement des messages Kafka
for message in consumer:
    data = message.value

    # Stockage dans MongoDB selon le topic
    if message.topic == "live_matches":
        # Upsert : mise à jour si match déjà existant
        col_matches.update_one(
            {"match_id": data["match_id"]},
            {"$set": data},
            upsert=True
        )
        print(f"Match enregistré/mis à jour dans MongoDB → {data['match_id']}")

    # Événements de match
    elif message.topic == "match_events":
        # Insertion simple (historique des événements)
        col_events.insert_one(data)
        print(f"Événement ajouté dans MongoDB → {data['match_id']}")
