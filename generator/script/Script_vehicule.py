import pandas as pd
from faker import Faker
import random
from datetime import datetime

# # Initilaisation de Faker
fake = Faker()

# Nombre de ligne a geenerer
N = 100

# Combinaisons cohérentes [type, compagnie, carburant]
combinations = [
    ['Taxi', 'Yango', 'Essence'],
    ['Taxi', 'Uber', 'Essence'],
    ['Taxi', 'Taxi Dakar', 'Diesel'],
    ['Moto', 'Yango Moto', 'Essence'],
    ['Moto', 'Moto Taxi', 'Essence'],
    ['Bus', 'Dakar Dem Dikk', 'Diesel'],
    ['Bus', 'BRT', 'Diesel'],
    ['Bus', 'AFTU', 'Electrique'],
    ['Train', 'TER', 'Electrique'],
    ['Car Rapide', 'Car Rapide Dakar', 'Diesel']
]

statuts = ['En service', 'Hors service', 'En maintenance', 'En panne', 'Garé']

# Génération des données
data = []
for i in range(N):
    # Choisir une combinaison cohérente
    combo = random.choice(combinations)
    
    vehicule = {
        'vehicule_id': fake.license_plate(),
        'type_transport': combo[0],
        'company_name': combo[1], 
        'capacity_fuel_type': combo[2],
        'status': random.choice(statuts)
    }
    data.append(vehicule)

df = pd.DataFrame(data)
df.to_csv('/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/script/donnees_vehicules.csv', index=False, encoding="utf-8")



print("Fichiers CSV générés avec succès.")

