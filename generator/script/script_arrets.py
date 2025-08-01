import pandas as pd
from faker import Faker
import random
from datetime import datetime

# Initialisation de Faker
fake = Faker()

# Nombre de lignes à générer
N = 100

# Définition des stops
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

# Mapping Arrêts → Stops
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

# Coordonnées fixes des arrêts
arret_coords = {
    'GUEDIAWAYE': (14.7828, -17.4003),
    'PIKINE': (14.7441, -17.4001),
    'THIAROYE': (14.7645, -17.3540),
    'MALIKA': (14.8022, -17.3280),
    'PARCELLE': (14.7590, -17.4480),
    'FANN': (14.6883, -17.4677),
    'OUAKAM': (14.7218, -17.4817),
    'SACRE COEUR': (14.7083, -17.4644),
    'yoff': (14.7564, -17.4906),
    'sicap': (14.7079, -17.4665),
    'sicap karack': (14.6999, -17.4577),
    'liberte 5': (14.7035, -17.4619),
    'liberte 6': (14.7055, -17.4592),
    'grand yoff': (14.7439, -17.4593),
    'grand dakar': (14.7048, -17.4550),
    'GUEULE TAPEE': (14.6819, -17.4519),
    'DIAKHAYE': (14.8032, -17.3190),
}

# Génération des données
data = []
for i in range(N):
    arret_name = random.choice(list(arret_stops.keys()))
    stop_id = random.choice(arret_stops[arret_name])

    latitude, longitude = arret_coords.get(arret_name, (fake.latitude(), fake.longitude()))

    arret = {
        'stop_id': stop_id,
        'name': arret_name,
        'latitude': latitude,
        'longitude': longitude,
        'zone': random.choice(['Zone Nord', 'Zone Sud', 'Zone Est', 'Zone Ouest', 'centre ville']),
        'shelter': random.choice(['true', 'false']),
    }
    data.append(arret)

df = pd.DataFrame(data)
df.to_csv(
    '/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/script/donnees_arrets.csv',
    index=False, encoding="utf-8"
)

print("Fichiers CSV générés avec succès.")
