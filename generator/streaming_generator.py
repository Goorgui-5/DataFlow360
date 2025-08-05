import time
import random
from datetime import datetime

def generer_evenement():
    return {
        "saison": random.choice([
            "2019-2020","2020-2021","2021-2022","2022-2023","2023-2024","2024-2025",
        ]),
        "equipe": "BarcelonE",  
        "pays": "ESPAGNE",
        "competition": "La Liga",
        "classement": str(random.randint(1, 3)),  
        "matchs_joues": random.randint(1, 38),
        "victoires": random.randint(20, 30),
        "nuls": random.randint(0, 10),
        "defaites": random.randint(0, 10),
        "buts_pour": random.randint(40, 110),
        "buts_contre": random.randint(20, 60),
        "difference_buts": random.randint(-10, 60),
        "points": random.randint(60, 100),
        "meilleur_buteur": random.choice([
            "Robert Lewandowski - 27", "Lionel Messi - 30",
            "Memphis Depay - 12", "Samuel Eto'o - 28", "Raphinha - 16", "Lamine Yamal - 20"
        ]),
        "gardien_de_but": random.choice([
            "Marc-André ter Stegen", "Iñaki Peña", "Wojciech Szczęsny", "Victor Valdés","Pinto"
        ])
    }

if __name__ == "__main__":
    print("🚀 Streaming des données 'saisons' du FC Barcelone (CTRL+C pour arrêter)")
    while True:
        evenement = generer_evenement()
        print(evenement)
        time.sleep(1)
