import time
import random
from datetime import datetime
from kafka import KafkaProducer
import json
import psycopg2

# Config Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Connexion PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,  
    database="dataflow360",
    user="postgres",
    password="nnbbvv"
)
cur = conn.cursor()

def generer_evenement():
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "saison": random.choice([
            "1990-1991","1991-1992","1992-1993","1993-1994","1994-1995","1995-1996","1996-1997",
            "1997-1998","1998-1999","1999-2000","2000-2001","2001-2002","2002-2003","2003-2004",
            "2004-2005","2005-2006","2006-2007","2007-2008","2008-2009","2009-2010","2010-2011",
            "2011-2012","2012-2013","2013-2014","2014-2015","2015-2016","2016-2017","2017-2018",
            "2018-2019","2019-2020","2020-2021","2021-2022","2022-2023","2023-2024"]),
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
            "Robert Lewandowski - 42", "Lionel Messi - 38", "Memphis Depay - 23", "Antoine Griezmann - 35",
            "Luis Suárez - 31", "Neymar Jr - 28", "Thierry Henry - 26", "David Villa - 32", 
            "Samuel Eto'o - 36", "Pierre-Emerick Aubameyang - 24"]),
        "gardien_de_but": random.choice([
            "Marc-André ter Stegen", "Iñaki Peña", "Wojciech Szczęsny", "Victor Valdés","Pinto"])
    }

# if __name__ == "__main__":
#     print("Streaming vers Kafka (topic: saisons_topic). CTRL+C pour arrêter")
#     while True:
#         evenement = generer_evenement()
#         print("Envoi à Kafka:", evenement)
#         producer.send("saisons_topic", evenement)
#         time.sleep(1)

if __name__ == "__main__":
    print("Streaming et insertion directe dans PostgreSQL (CTRL+C pour arrêter)")
    try:
        while True:
            evenement = generer_evenement()
            print("Envoi à Kafka :", evenement)

            cur.execute("""
                INSERT INTO saisons (
                    saison, equipe, pays, competition, classement,
                    matchs_joues, victoires, nuls, defaites,
                    buts_pour, buts_contre, difference_buts,
                    points, meilleur_buteur, gardien_de_but
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                evenement["saison"], evenement["equipe"], evenement["pays"],
                evenement["competition"], evenement["classement"],
                evenement["matchs_joues"], evenement["victoires"], evenement["nuls"],
                evenement["defaites"], evenement["buts_pour"], evenement["buts_contre"],
                evenement["difference_buts"], evenement["points"],
                evenement["meilleur_buteur"], evenement["gardien_de_but"]
            ))
            
            conn.commit()
            producer.send("saisons_topic", evenement)
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n Arrêt du streaming")
    finally:
        cur.close()
        conn.close()



# if __name__ == "__main__":
#     print("Streaming des données 'saisons' du FC Barcelone (CTRL+C pour arrêter)")
#     while True:
#         evenement = generer_evenement()
#         print(evenement)
#         time.sleep(1)











# ================================================================================================

# import time
# import random
# from datetime import datetime
# import psycopg2

# # Connexion PostgreSQL
# conn = psycopg2.connect(
#     host="localhost",
#     port=5432,  
#     database="dataflow360",
#     user="postgres",
#     password="nnbbvv"
# )
# cur = conn.cursor()

# def generer_evenement():
#     return {
#         "saison": random.choice([
#             "1990-1991","1991-1992","1992-1993","1993-1994","1994-1995","1995-1996","1996-1997",
#             "1997-1998","1998-1999","1999-2000","2000-2001","2001-2002","2002-2003","2003-2004",
#             "2004-2005","2005-2006","2006-2007","2007-2008","2008-2009","2009-2010","2010-2011",
#             "2011-2012","2012-2013","2013-2014","2014-2015","2015-2016","2016-2017","2017-2018",
#             "2018-2019","2019-2020","2020-2021","2021-2022","2022-2023","2023-2024"]),
#         "equipe": "BarcelonE",  
#         "pays": "ESPAGNE",
#         "competition": "La Liga",
#         "classement": str(random.randint(1, 3)),  
#         "matchs_joues": random.randint(1, 38),
#         "victoires": random.randint(20, 30),
#         "nuls": random.randint(0, 10),
#         "defaites": random.randint(0, 10),
#         "buts_pour": random.randint(40, 110),
#         "buts_contre": random.randint(20, 60),
#         "difference_buts": random.randint(-10, 60),
#         "points": random.randint(60, 100),
#         "meilleur_buteur": random.choice([
#             "Robert Lewandowski - 42", "Lionel Messi - 38", "Memphis Depay - 23", "Antoine Griezmann - 35",
#             "Luis Suárez - 31", "Neymar Jr - 28", "Thierry Henry - 26", "David Villa - 32", 
#             "Samuel Eto'o - 36", "Pierre-Emerick Aubameyang - 24"]),
#         "gardien_de_but": random.choice([
#             "Marc-André ter Stegen", "Iñaki Peña", "Wojciech Szczęsny", "Victor Valdés","Pinto"
#         ])
#     }

# if __name__ == "__main__":
#     print("Streaming et insertion directe dans PostgreSQL (CTRL+C pour arrêter)")
#     try:
#         while True:
#             evenement = generer_evenement()
#             print("Nouvel événement :", evenement)

#             cur.execute("""
#                 INSERT INTO saisons (
#                     saison, equipe, pays, competition, classement,
#                     matchs_joues, victoires, nuls, defaites,
#                     buts_pour, buts_contre, difference_buts,
#                     points, meilleur_buteur, gardien_de_but
#                 )
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
#             """, (
#                 evenement["saison"], evenement["equipe"], evenement["pays"],
#                 evenement["competition"], evenement["classement"],
#                 evenement["matchs_joues"], evenement["victoires"], evenement["nuls"],
#                 evenement["defaites"], evenement["buts_pour"], evenement["buts_contre"],
#                 evenement["difference_buts"], evenement["points"],
#                 evenement["meilleur_buteur"], evenement["gardien_de_but"]
#             ))
            
#             conn.commit()
#             time.sleep(3)
#     except KeyboardInterrupt:
#         print("\n Arrêt du streaming")
#     finally:
#         cur.close()
#         conn.close()
# ================================================================================================