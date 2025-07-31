import pandas as pd
from faker import Faker
import random
from datetime import datetime

# Initialisation de Faker
fake = Faker()

# Nombre de lignes à générer
N = 100

stop = [
    "Stop_01", "Stop_01", "Stop_08", "Stop_01", "Stop_06", "Stop_02", "Stop_02", 
    "Stop_06", "Stop_012", "Stop_08", "Stop_04", "Stop_01", "Stop_015", "Stop_014", 
    "Stop_06", "Stop_04", "Stop_04", "Stop_05", "Stop_011", "Stop_08", "Stop_011", 
    "Stop_02", "Stop_03", "Stop_09", "Stop_010", "Stop_015", "Stop_01", "Stop_013", 
    "Stop_010", "Stop_010", "Stop_011", "Stop_011", "Stop_015", "Stop_014", "Stop_010", 
    "Stop_05", "Stop_014", "Stop_06", "Stop_04", "Stop_014", "Stop_03", "Stop_015", 
    "Stop_02", "Stop_014", "Stop_06", "Stop_02", "Stop_01", "Stop_015", "Stop_012", 
    "Stop_010", "Stop_010", "Stop_012", "Stop_07", "Stop_05", "Stop_015", "Stop_04", 
    "Stop_03", "Stop_014", "Stop_015", "Stop_09", "Stop_011", "Stop_07", "Stop_010", 
    "Stop_05", "Stop_05", "Stop_012", "Stop_010", "Stop_07", "Stop_013", "Stop_07", 
    "Stop_014", "Stop_07", "Stop_02", "Stop_02", "Stop_04", "Stop_08", "Stop_010", 
    "Stop_01", "Stop_015", "Stop_04", "Stop_014", "Stop_09", "Stop_015", "Stop_07", 
    "Stop_01", "Stop_03", "Stop_011", "Stop_012", "Stop_02", "Stop_010", "Stop_013", 
    "Stop_014", "Stop_06", "Stop_03", "Stop_06", "Stop_012", "Stop_06", "Stop_03", 
    "Stop_012", "Stop_01"
]

# Définir quels stops appartiennent à quels arrêts
arret_stops = {
    'GUEULE TAPEE': ['Stop_01', 'Stop_02', 'Stop_03'],
    'GUEDIAWAYE': ['Stop_04', 'Stop_05', 'Stop_06'],
    'DIEUPPEUL': ['Stop_07', 'Stop_08', 'Stop_09'],
    'PIKINE': ['Stop_010', 'Stop_011', 'Stop_012'],
    'THIAROYE': ['Stop_013', 'Stop_014', 'Stop_015'],
    'PARCELLE': ['Stop_01', 'Stop_04', 'Stop_07'],
    'DIAKHAYE': ['Stop_02', 'Stop_05', 'Stop_08'],
    'MALIKA': ['Stop_03', 'Stop_06', 'Stop_09'],
    'SACRE COEUR': ['Stop_010', 'Stop_013', 'Stop_01'],
    'FANN': ['Stop_011', 'Stop_014', 'Stop_02'],
    'OUAKAM': ['Stop_012', 'Stop_015', 'Stop_03'],
    'grand yoff': ['Stop_04', 'Stop_07', 'Stop_010'],
    'yoff': ['Stop_05', 'Stop_08', 'Stop_011'],
    'sicap': ['Stop_06', 'Stop_09', 'Stop_012'],
    'sicap karack': ['Stop_013', 'Stop_01', 'Stop_04'],
    'liberte 5': ['Stop_014', 'Stop_02', 'Stop_05'],
    'liberte 6': ['Stop_015', 'Stop_03', 'Stop_06'],
    'grand dakar': ['Stop_07', 'Stop_08', 'Stop_09']
}

# Génération des données
data = []
for i in range(N):
    # Choisir d'abord un arrêt
    arret_name = random.choice(list(arret_stops.keys()))
    # Choisir un stop cohérent avec cet arrêt
    stop_id = random.choice(arret_stops[arret_name])
    
    arret = {
        'stop_id': stop_id,
        'name': arret_name,
        'latitude': fake.latitude(),
        'longitude': fake.longitude(),
        'zone': random.choice(['Zone Nord', 'Zone Sud', 'Zone Est', 'Zone Ouest', 'centre ville']),
        'shelter': random.choice(['true', 'false']),
    }
    data.append(arret)

df = pd.DataFrame(data)
df.to_csv('/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/script/donnees_arrets.csv', index=False, encoding="utf-8")

print("Fichiers CSV générés avec succès.")