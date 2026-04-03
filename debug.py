from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get("https://www.flashscore.fr/football/senegal/ligue-1/resultats/")
time.sleep(5)

elements = driver.find_elements(By.CLASS_NAME, "event__match")
for el in elements[:5]:
    try:
        date = el.find_element(By.CLASS_NAME, "event__time").text
        dom  = el.find_element(By.CLASS_NAME, "event__homeParticipant").text
        ext  = el.find_element(By.CLASS_NAME, "event__awayParticipant").text
        print(f"DATE: '{date}' | {dom} vs {ext}")
    except Exception as e:
        print(f"Erreur: {e}")

driver.quit()