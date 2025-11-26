# consumers/kafka_to_redis.py
import json
import redis
from kafka import KafkaConsumer

# Connexion à Redis
r = redis.Redis(host="localhost", port=6379, db=0)

# Configuration Kafka (écoute des 2 topics)
consumer = KafkaConsumer(
    'live_matches',
    'match_events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='redis-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(" En attente des messages Kafka...")

# Durée de vie des données dans Redis (en secondes)
TTL = 10800 # 1 heure

# Traitement des messages Kafka
for message in consumer:
    data = message.value

    # Stockage dans Redis selon le topic
    if message.topic == "live_matches":
        key = f"match:{data['match_id']}"
        r.set(key, json.dumps(data))
        r.expire(key, TTL)  # suppression automatique après TTL
        print(f" Match stocké → {key}")
    
    # Stockage des événements de match
    elif message.topic == "match_events":
        key = f"events:{data['match_id']}"
        r.rpush(key, json.dumps(data))  # ajout à la liste
        r.expire(key, TTL)  # liste supprimée après TTL
        print(f" Événement ajouté → {key}")
