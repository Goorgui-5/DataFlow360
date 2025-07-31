import pandas as pd
from faker import Faker
import random
from datetime import datetime

# # Initilaisation de Faker
fake = Faker()

# Nombre de ligne a geenerer
N = 100

id_vehicule = [
    "40871", "5MB7090", "EW0 1288", "159-SPK", "6KKI947", "CK-1635", 
    "4CE 163", "386WB", "98O GK3", "9KC B23", "10-RR80", "EI 8495", 
    "KNM 372", "9083 TJ", "90-HZ42", "MMS 847", "JHR-7084", "5S 4P3EAP", 
    "DX 7275", "634 LGE", "447 VWU", "GV6 D4F", "KCZ-816", "LNI 946", 
    "3O 7723F", "YSW-5458", "766 HJY", "8E-YXTC", "541 DTM", "ZFI 852", 
    "NJO 097", "E05 0WH", "0N714", "RSS 3235", "52-83272", "T79 7QX", 
    "EX2 4253", "NR6 V2S", "1SX 956", "484-PFX", "LQ 5837", "8VO11", 
    "01-RI69", "1-T5687", "MIB 783", "40-9380L", "33-LA76", "2HT3948", 
    "QAE 968", "ZDL-682", "329 ZRJ", "02R-LBHA", "35ZU8", "97Z 310", 
    "96-25945", "368 BAJ", "0KYC670", "TFG-868", "9H 48036", "E53-ZUZ", 
    "GKF7934", "264L2", "2Z 1217P", "12Q 777", "415 OVP", "6A 7B7FZY", 
    "PVO 6936", "24XO4", "FOK2951", "9Z-CMPO", "1-58409", "575 HNG", 
    "4H 9498A", "A20-ZYN", "937SAT", "585-OZS", "53Z R83", "LAB 884", 
    "97FS152", "1PG 083", "4-59428Q", "65-07417", "EFO-4867", "HGS 008", 
    "804 9RT", "M89 4DR", "181 KSW", "853 6579", "256CB", "6-5951I", 
    "SWA-630", "53-K964", "JVR-403", "77-X478", "051 1VS", "8NI L64", 
    "1IJ 068", "34BY1", "45Q E65", "147-QGZR"
]

# Génération des données
data = []
for i in range(N):

    Position_GPS = {
        'route_id': random.choice(['R1', 'R2', 'R3', 'R4', 'R5', 'R6','R7', 'R8', 'R9', 'R10', 'R11', 'R12']),
        'vehicule_id': random.choice(id_vehicule),
        'timestamp': fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        'longitude': fake.longitude(),
        'speed_kmh': random.randint(0, 120),
        'direction': random.choice(['Nord', 'Sud', 'Est', 'Ouest', 'Nord-Est', 'Nord-Ouest', 'Sud-Est', 'Sud-Ouest']),
        'traffic': random.choice(['Fluide', 'moyen', 'bouchonné','bloqué']),
    }
    data.append(Position_GPS)

df = pd.DataFrame(data)
df.to_csv('/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/script/donnees_Position_GPS.csv', index=False, encoding="utf-8")

print("Fichiers CSV générés avec succès.")

