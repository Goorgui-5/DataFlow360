import pandas as pd
from faker import Faker
import random
from datetime import datetime

# Initilaisation de Faker
fake = Faker()

# Nombre de ligne a geenerer
N = 500

#Generation des donnees
data = []
for i in range(N):
    record = {
        'id': i + 1,
        'nom_joueur': fake.last_name(),
        'prenom_joueur': fake.first_name(),
        'age': fake.random_int( min=15, max=42),
        'pied': random.choice(['Droitier', 'Gaucher']),
        'poste': random.choice(['Gardien', 'Défenseur', 'Milieu','Attaquant']),
        'pays': fake.country(),
        'club': fake.company(),
        'Nb_matchs': fake.random_int(min=0, max=900),
        'Nb_buts_marques': fake.random_int(min=0, max=900),
        'Nb_passes': fake.random_int(min=0, max=500),
    }
    data.append(record)
    
# conversion en DataFrame
df = pd.DataFrame(data)

# Sauvegarde du DataFrame dans un fichier CSV
df.to_csv('/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/donnees_batch.csv', index=False, encoding="utf-8")

# Sauvegarde du DataFrame dans un fichier JSON
df.to_json('/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/donnees_batch.json', orient='records', lines=True, force_ascii=False)

print("Fichiers CSV et JSON générés avec succès.")