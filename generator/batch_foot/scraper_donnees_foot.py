import requests
from bs4 import BeautifulSoup
import csv
import time

URL = "https://fbref.com/en/squads/206d90db/history/Barcelona-Stats-and-History"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

response = requests.get(URL, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')
time.sleep(2)

# Créer le CSV
with open('Donnees_Foot.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Saison','Equipe','Pays', 'Comp','Lgrang', 'disputé','W','D','L','GF','Georgie','GD','Pts','Présence',"Meilleur buteur'de l'equipe",'Gardien de but','Remarques'])

    # Parcourir chaque ligne du tableau
    for row in soup.find_all('tr')[1:]:
        # Récupérer à la fois th et td
        cells = row.find_all(['th', 'td'])
        
        # Prendre les 18 premières cellules
        data = []
        for cell in cells[:18]:
            text = cell.text.strip()
            # Mettre NA si vide
            if text == "":
                text = "NA"
            data.append(text)
        
        # Écrire dans le CSV
        if len(data) >= 10:
            writer.writerow(data)

print("Terminé! Fichier: Donnees_Foot.csv")