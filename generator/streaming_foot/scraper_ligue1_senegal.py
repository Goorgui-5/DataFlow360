import time
import psycopg2
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# CONFIG DB
# =========================
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "dataflow360",
    "user":     "postgres",
    "password": "nnbbvv"
}

URL = "https://www.flashscore.fr/football/senegal/ligue-1/resultats/"

# =========================
# MAPPING NOMS EQUIPES
# =========================
TEAM_MAPPING = {
    "AJEL":              "AJEL Rufisque",
    "AS Camberene":      "ASC Cambérène",
    "HLM de Dakar":      "ASC HLM",
    "Jaraaf":            "ASC Jaraaf",
    "Pikine":            "AS Pikine",
    "Dakar SC":          "Dakar Sacré-Cœur",
    "Generation Foot":   "Génération Foot",
    "Guediawaye FC":     "Guédiawaye FC",
    "Linguere":          "Linguère",
    "Stade Mbour":       "Stade de Mbour",
    "US Goree":          "US Gorée",
    "Wally Daan":        "Wallydaan",
}

def normalize_team(name):
    name = name.strip()
    return TEAM_MAPPING.get(name, name)

# =========================
# PARSER DATE
# =========================
def parse_date(date_str):
    """
    Flashscore utilise deux formats :
    - Matchs recents  : "15.03. 16:30"  (sans annee, avec heure)
    - Anciens matchs  : "29.12.2025"    (avec annee, sans heure)
    """
    try:
        date_str = date_str.strip()
        annee = datetime.now().year
        if ". " in date_str:
            date_str_clean = date_str.replace(". ", f".{annee} ")
            date_obj = datetime.strptime(date_str_clean, "%d.%m.%Y %H:%M")
            return date_obj.strftime("%Y-%m-%d %H:%M:%S")
        elif len(date_str) == 10 and date_str.count(".") == 2:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            return date_obj.strftime("%Y-%m-%d 17:00:00")
        else:
            return None
    except:
        return None

def parse_date_OLD(date_str):
    """
    Format Flashscore : "15.03. 16:30"
    → "15.03.2026 16:30" → "2026-03-15 16:30:00"
    """
    try:
        date_str = date_str.strip()
        annee = datetime.now().year
        # "15.03. 16:30" → "15.03.2026 16:30"
        date_str_clean = date_str.replace(". ", f".{annee} ")
        date_obj = datetime.strptime(date_str_clean, "%d.%m.%Y %H:%M")
        return date_obj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"  ⚠️ Parse date echoue pour '{date_str}' : {e}")
        return None  # On retourne None pour ignorer ce match

# =========================
# DRIVER SELENIUM
# =========================
def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

# =========================
# SCRAPING
# =========================
def scrape_resultats():
    print("Lancement du scraper Flashscore — Ligue 1 Senegal...")
    driver = get_driver()
    matchs = []
    date_courante = None  # Garde en mémoire la dernière date valide

    try:
        driver.get(URL)
        print("Page chargee")

        # Cookies
        try:
            cookie_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            cookie_btn.click()
            time.sleep(2)
        except:
            pass

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "event__match"))
        )
        time.sleep(3)

        elements = driver.find_elements(By.CLASS_NAME, "event__match")
        print(f"{len(elements)} matchs trouves sur Flashscore")

        for el in elements:
            try:
                # Date
                try:
                    date_str = el.find_element(By.CLASS_NAME, "event__time").text.strip()
                    parsed = parse_date(date_str)
                    if parsed:
                        date_courante = parsed  # Met à jour la date courante
                except:
                    pass

                # Si on n'a pas de date valide, on ignore ce match
                if not date_courante:
                    continue

                # Equipes normalisées
                equipe_dom = normalize_team(
                    el.find_element(By.CLASS_NAME, "event__homeParticipant").text
                )
                equipe_ext = normalize_team(
                    el.find_element(By.CLASS_NAME, "event__awayParticipant").text
                )

                # Scores
                try:
                    score_dom = int(el.find_element(By.CLASS_NAME, "event__score--home").text.strip())
                    score_ext = int(el.find_element(By.CLASS_NAME, "event__score--away").text.strip())
                    statut = "FINISHED"
                except:
                    score_dom = None
                    score_ext = None
                    statut = "TIMED"

                matchs.append({
                    "competition": "Ligue 1 Senegal",
                    "date_heure":  date_courante,
                    "equipe_dom":  equipe_dom,
                    "equipe_ext":  equipe_ext,
                    "score_dom":   score_dom,
                    "score_ext":   score_ext,
                    "statut":      statut,
                })

            except Exception as e:
                print(f"Erreur sur un match : {e}")
                continue

    except Exception as e:
        print(f"Erreur scraping : {e}")
    finally:
        driver.quit()

    print(f"{len(matchs)} matchs scrapes avec succes")
    return matchs

# =========================
# INSERTION
# =========================
def insert_matchs(matchs, last_match_id):
    if not matchs:
        print("Aucun match a inserer")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0
    updated  = 0
    skipped  = 0
    current_match_id = last_match_id

    for m in matchs:
        try:
            # Vérifier si le match existe déjà
            cursor.execute("""
                SELECT id FROM matchs_en_direct
                WHERE equipe_dom  = %s 
                AND equipe_ext    = %s
                AND DATE(date_heure) = DATE(%s)
                AND competition   = %s
            """, (m["equipe_dom"], m["equipe_ext"], m["date_heure"], m["competition"]))

            existing = cursor.fetchone()

            if existing:
                # UPDATE score et statut
                cursor.execute("""
                    UPDATE matchs_en_direct
                    SET score_dom    = %s,
                        score_ext    = %s,
                        statut       = %s,
                        last_updated = NOW()
                    WHERE id = %s
                """, (m["score_dom"], m["score_ext"], m["statut"], existing[0]))
                updated += 1

            else:
                # INSERT avec match_id séquentiel
                current_match_id += 1
                cursor.execute("""
                    INSERT INTO matchs_en_direct 
                    (match_id, competition, date_heure, equipe_dom, equipe_ext,
                     score_dom, score_ext, statut, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    current_match_id,
                    m["competition"],
                    m["date_heure"],
                    m["equipe_dom"],
                    m["equipe_ext"],
                    m["score_dom"],
                    m["score_ext"],
                    m["statut"]
                ))
                inserted += 1
                print(f"  + match_id={current_match_id} : {m['equipe_dom']} vs {m['equipe_ext']} ({m['date_heure']})")

        except Exception as e:
            print(f"Erreur insertion : {e}")
            conn.rollback()
            skipped += 1
            continue

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n✅ {inserted} inseres | {updated} mis a jour | {skipped} ignores")
    if inserted > 0:
        print(f"   match_id {last_match_id + 1} → {current_match_id}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print("=" * 55)
    print("  SCRAPER LIGUE 1 SENEGAL — FootStream (CLEAN)")
    print("=" * 55)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT MAX(match_id), MAX(date_heure) 
        FROM matchs_en_direct 
        WHERE competition LIKE '%Senegal%'
    """)
    row = cursor.fetchone()
    last_match_id = row[0] or 112
    last_date     = row[1]

    cursor.close()
    conn.close()

    print(f"Dernier match_id : {last_match_id}")
    print(f"Derniere date    : {last_date}")
    print(f"Prochain id      : {last_match_id + 1}\n")

    matchs = scrape_resultats()

    if matchs:
        print("\nApercu (5 premiers) :")
        for m in matchs[:5]:
            print(f"  {m['date_heure']} | {m['equipe_dom']} vs {m['equipe_ext']} | {m['statut']}")
        print()
        insert_matchs(matchs, last_match_id)
    else:
        print("Aucun match recupere")

    print("\nScraping termine !")


# import time
# import psycopg2
# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

# # =========================
# # CONFIG DB
# # =========================
# DB_CONFIG = {
#     "host":     "localhost",
#     "port":     5432,
#     "database": "dataflow360",
#     "user":     "postgres",
#     "password": "nnbbvv"
# }

# URL = "https://www.flashscore.fr/football/senegal/ligue-1/resultats/"

# # =========================
# # MAPPING NOMS EQUIPES
# # =========================
# TEAM_MAPPING = {
#     "AJEL":              "AJEL Rufisque",
#     "AS Camberene":      "ASC Cambérène",
#     "HLM de Dakar":      "ASC HLM",
#     "Jaraaf":            "ASC Jaraaf",
#     "Pikine":            "AS Pikine",
#     "Dakar SC":          "Dakar Sacré-Cœur",
#     "Generation Foot":   "Génération Foot",
#     "Guediawaye FC":     "Guédiawaye FC",
#     "Linguere":          "Linguère",
#     "Stade Mbour":       "Stade de Mbour",
#     "US Goree":          "US Gorée",
#     "Wally Daan":        "Wallydaan",
# }

# def normalize_team(name):
#     name = name.strip()
#     return TEAM_MAPPING.get(name, name)

# # =========================
# # PARSER DATE
# # =========================
# def parse_date(date_str):
#     """
#     Format Flashscore : "15.03. 16:30"
#     → "15.03.2026 16:30" → "2026-03-15 16:30:00"
#     """
#     try:
#         date_str = date_str.strip()
#         annee = datetime.now().year
#         # "15.03. 16:30" → "15.03.2026 16:30"
#         date_str_clean = date_str.replace(". ", f".{annee} ")
#         date_obj = datetime.strptime(date_str_clean, "%d.%m.%Y %H:%M")
#         return date_obj.strftime("%Y-%m-%d %H:%M:%S")
#     except Exception as e:
#         print(f"  ⚠️ Parse date echoue pour '{date_str}' : {e}")
#         return None  # On retourne None pour ignorer ce match

# # =========================
# # DRIVER SELENIUM
# # =========================
# def get_driver():
#     options = Options()
#     options.add_argument("--headless")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--window-size=1920,1080")
#     options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
#     service = Service(ChromeDriverManager().install())
#     return webdriver.Chrome(service=service, options=options)

# # =========================
# # SCRAPING
# # =========================
# def scrape_resultats():
#     print("Lancement du scraper Flashscore — Ligue 1 Senegal...")
#     driver = get_driver()
#     matchs = []
#     date_courante = None  # Garde en mémoire la dernière date valide

#     try:
#         driver.get(URL)
#         print("Page chargee")

#         # Cookies
#         try:
#             cookie_btn = WebDriverWait(driver, 5).until(
#                 EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
#             )
#             cookie_btn.click()
#             time.sleep(2)
#         except:
#             pass

#         WebDriverWait(driver, 15).until(
#             EC.presence_of_element_located((By.CLASS_NAME, "event__match"))
#         )
#         time.sleep(3)

#         elements = driver.find_elements(By.CLASS_NAME, "event__match")
#         print(f"{len(elements)} matchs trouves sur Flashscore")

#         for el in elements:
#             try:
#                 # Date
#                 try:
#                     date_str = el.find_element(By.CLASS_NAME, "event__time").text.strip()
#                     parsed = parse_date(date_str)
#                     if parsed:
#                         date_courante = parsed  # Met à jour la date courante
#                 except:
#                     pass

#                 # Si on n'a pas de date valide, on ignore ce match
#                 if not date_courante:
#                     continue

#                 # Equipes normalisées
#                 equipe_dom = normalize_team(
#                     el.find_element(By.CLASS_NAME, "event__homeParticipant").text
#                 )
#                 equipe_ext = normalize_team(
#                     el.find_element(By.CLASS_NAME, "event__awayParticipant").text
#                 )

#                 # Scores
#                 try:
#                     score_dom = int(el.find_element(By.CLASS_NAME, "event__score--home").text.strip())
#                     score_ext = int(el.find_element(By.CLASS_NAME, "event__score--away").text.strip())
#                     statut = "FINISHED"
#                 except:
#                     score_dom = None
#                     score_ext = None
#                     statut = "TIMED"

#                 matchs.append({
#                     "competition": "Ligue 1 Senegal",
#                     "date_heure":  date_courante,
#                     "equipe_dom":  equipe_dom,
#                     "equipe_ext":  equipe_ext,
#                     "score_dom":   score_dom,
#                     "score_ext":   score_ext,
#                     "statut":      statut,
#                 })

#             except Exception as e:
#                 print(f"Erreur sur un match : {e}")
#                 continue

#     except Exception as e:
#         print(f"Erreur scraping : {e}")
#     finally:
#         driver.quit()

#     print(f"{len(matchs)} matchs scrapes avec succes")
#     return matchs

# # =========================
# # INSERTION
# # =========================
# def insert_matchs(matchs, last_match_id):
#     if not matchs:
#         print("Aucun match a inserer")
#         return

#     conn = psycopg2.connect(**DB_CONFIG)
#     cursor = conn.cursor()

#     inserted = 0
#     updated  = 0
#     skipped  = 0
#     current_match_id = last_match_id

#     for m in matchs:
#         try:
#             # Vérifier si le match existe déjà
#             cursor.execute("""
#                 SELECT id FROM matchs_en_direct
#                 WHERE equipe_dom  = %s 
#                 AND equipe_ext    = %s
#                 AND DATE(date_heure) = DATE(%s)
#                 AND competition   = %s
#             """, (m["equipe_dom"], m["equipe_ext"], m["date_heure"], m["competition"]))

#             existing = cursor.fetchone()

#             if existing:
#                 # UPDATE score et statut
#                 cursor.execute("""
#                     UPDATE matchs_en_direct
#                     SET score_dom    = %s,
#                         score_ext    = %s,
#                         statut       = %s,
#                         last_updated = NOW()
#                     WHERE id = %s
#                 """, (m["score_dom"], m["score_ext"], m["statut"], existing[0]))
#                 updated += 1

#             else:
#                 # INSERT avec match_id séquentiel
#                 current_match_id += 1
#                 cursor.execute("""
#                     INSERT INTO matchs_en_direct 
#                     (match_id, competition, date_heure, equipe_dom, equipe_ext,
#                      score_dom, score_ext, statut, last_updated)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
#                 """, (
#                     current_match_id,
#                     m["competition"],
#                     m["date_heure"],
#                     m["equipe_dom"],
#                     m["equipe_ext"],
#                     m["score_dom"],
#                     m["score_ext"],
#                     m["statut"]
#                 ))
#                 inserted += 1
#                 print(f"  + match_id={current_match_id} : {m['equipe_dom']} vs {m['equipe_ext']} ({m['date_heure']})")

#         except Exception as e:
#             print(f"Erreur insertion : {e}")
#             conn.rollback()
#             skipped += 1
#             continue

#     conn.commit()
#     cursor.close()
#     conn.close()

#     print(f"\n✅ {inserted} inseres | {updated} mis a jour | {skipped} ignores")
#     if inserted > 0:
#         print(f"   match_id {last_match_id + 1} → {current_match_id}")

# # =========================
# # MAIN
# # =========================
# if __name__ == "__main__":
#     print("=" * 55)
#     print("  SCRAPER LIGUE 1 SENEGAL — FootStream (CLEAN)")
#     print("=" * 55)

#     conn = psycopg2.connect(**DB_CONFIG)
#     cursor = conn.cursor()

#     cursor.execute("""
#         SELECT MAX(match_id), MAX(date_heure) 
#         FROM matchs_en_direct 
#         WHERE competition LIKE '%Senegal%'
#     """)
#     row = cursor.fetchone()
#     last_match_id = row[0] or 112
#     last_date     = row[1]

#     cursor.close()
#     conn.close()

#     print(f"Dernier match_id : {last_match_id}")
#     print(f"Derniere date    : {last_date}")
#     print(f"Prochain id      : {last_match_id + 1}\n")

#     matchs = scrape_resultats()

#     if matchs:
#         print("\nApercu (5 premiers) :")
#         for m in matchs[:5]:
#             print(f"  {m['date_heure']} | {m['equipe_dom']} vs {m['equipe_ext']} | {m['statut']}")
#         print()
#         insert_matchs(matchs, last_match_id)
#     else:
#         print("Aucun match recupere")

#     print("\nScraping termine !")
