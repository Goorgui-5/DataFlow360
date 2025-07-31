import requests
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime
import time

URL = "https://www.transit.land/operators"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

response = requests.get(URL, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')
time.sleep(3)

# Créer le CSV
with open('operators.json', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Nom', 'Ville', 'Etat', 'Pays'])

    # Parcourir chaque ligne du tableau
    for row in soup.find_all('tr')[1:]:
        cells = row.find_all('td')
        
        # Prendre les 4 premières cellules
        data = []
        for cell in cells[:4]:
            data.append(cell.text.strip())
        
        # Écrire dans le CSV
        if len(data) == 4:
            writer.writerow(data)

print("Terminé! Fichier: operators.json") 