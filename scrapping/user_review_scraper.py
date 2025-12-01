#%%
import os
import time, random
from datetime import datetime
import re
import unicodedata
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, SessionNotCreatedException

import logging
import re


from create_db import insertRow,query_db

BASE_URL = "https://www.metacritic.com/game"
options = Options()
options.add_argument('--headless')
options.add_argument('enable-automation')
options.add_argument('--no-sandbox')
options.add_argument('--disable-extensions')
options.add_argument('--dns-prefetch-disable')
options.add_argument('--disable-gpu')
options.add_argument("--window-size=1280,2200")  # importante para lazy-load
options.page_load_strategy = "eager"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

COOLDOWN_SECS = 300  # 5 minutos
MAX_NEWSESSION_RETRIES = 3
MAX_GET_RETRIES = 3


def limpiar(texto, permitidos="!-_"):
    # 1. normalizar a NFD (descompone letras + acentos)
    texto = unicodedata.normalize("NFD", texto)
    # 2. filtrar marcas de acento (categoría Mn = Mark, Nonspacing)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")

    # 3. reemplazar espacio por guion
    texto = texto.replace(" ", "-")

    # 4. escapamos los caracteres permitidos para regex
    chars = re.escape(permitidos)
    patron = f"[^A-Za-z0-9{chars}]" 

    return re.sub(patron, "", texto)


def is_session_boot_error(err: Exception) -> bool:
    s = str(err).lower()
    return (
        isinstance(err, SessionNotCreatedException)
        or "chrome not reachable" in s
        or "disconnected" in s
        or "devtoolsactiveport" in s
        or "session not created" in s
    )

def build_options() -> Options:
    opts = Options()
    # Empezá sencillo; agregá flags luego si querés
    # opts.add_argument("--headless=new")  # activalo recién si ya levanta sin headless
    # opts.add_argument("--disable-quic")
    # opts.add_argument("--disable-blink-features=AutomationControlled")
    # opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    # opts.add_experimental_option("useAutomationExtension", False)
    return opts

def make_driver_with_retry() -> webdriver.Chrome:
    last_exc = None
    for attempt in range(1, MAX_NEWSESSION_RETRIES + 1):
        try:
            # options = build_options()
            driver = webdriver.Chrome(service=Service("./chromedriver-win64/chromedriver.exe"), options=options)
            driver.set_page_load_timeout(120)
            return driver
        except Exception as e:
            last_exc = e
            if is_session_boot_error(e) and attempt < MAX_NEWSESSION_RETRIES:
                logger.warning(f"[make_driver] Falló crear sesión (intento {attempt}/{MAX_NEWSESSION_RETRIES}). "
                               f"Dormimos {COOLDOWN_SECS//60} min y reintentamos. Error: {e}")
                time.sleep(COOLDOWN_SECS)
                continue
            raise
    raise last_exc  # por si acaso

def safe_get(driver, url: str) -> None:
    last = None
    for i in range(1, MAX_GET_RETRIES + 1):
        try:
            driver.get("about:blank")
            time.sleep(0.3)
            driver.get(url)
            return
        except (TimeoutException, WebDriverException) as e:
            last = e
            s = str(e).lower()
            # si es un corte de conexión / antibot, podés dormir corto y reintentar
            if i < MAX_GET_RETRIES and ("err_connection_closed" in s or "timeout" in s or "timed out" in s):
                time.sleep(random.uniform(2, 5))
                continue
            raise
    if last:
        raise last

def scrape_reviews(id_juego):
    """
    Scrapea las reseñas de un juego en Metacritic

    Parameters
    ----------
    id_juego : str
        ID del juego en Metacritic

    Returns
    -------
    list of tuples
        Cada tuple contiene la información de una reseña: (id_usuario, score, texto, fecha_sql)
    """
    reviews_url = f'{BASE_URL}/{id_juego}/user-reviews/'
    driver = None
    try:
        driver = make_driver_with_retry()
        wait = WebDriverWait(driver, 20)

        try:
            safe_get(driver, reviews_url)
        except TimeoutException:
            driver.execute_script("window.stop();")

        # esperar a que aparezcan reviews iniciales
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '[data-testid="review-card"], .c-siteReview_main')
            ))
        except TimeoutException:
            pass

        # scroll simple
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(3000): #hasta 30mil comentarios
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            time.sleep(1.5)
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        page_content = driver.page_source
    finally:
        if driver is not None:
            try: driver.quit()
            except: pass

    soup = BeautifulSoup(page_content, "lxml")

    reviews = []
    review_elements = soup.find_all('div', class_='c-siteReview_main')
    s = ""

    for review in review_elements:
        user = review.find('a', class_='c-siteReviewHeader_username').get_text(strip=True)               
        user = limpiar(user) #Convertir a id_usuario aceptable para scrapping futuro
        
        score = review.find('div', class_='c-siteReviewScore').get_text(strip=True)
        review_text = review.find('div', class_='c-siteReview_quote').get_text(strip=True)
        
        review_date = review.find('div', class_='c-siteReview_reviewDate')
        if review_date and review_date.get_text(strip=True) is not s:
            fecha_obj = datetime.strptime(review_date.get_text(strip=True), "%b %d, %Y")   # parsea string → datetime
            fecha_sql = fecha_obj.strftime("%Y-%m-%d")
        else:
            fecha_sql = None 
            logger.warning("Fecha de review no encontrada")        
              
        reviews.append((user, score, review_text, fecha_sql))
    return reviews
    
#%%
if __name__ == "__main__":
    # Se agrega el nombre de juego al dataframe
    ls_urls = pd.read_csv("data/urls_metacritic.csv", sep="|")
    ls_urls["id_juego"] = ls_urls["url"].apply(os.path.basename)

    # Se obtienen los juegos de la base de datos
    query_ID_JUEGO = "SELECT DISTINCT id_juego FROM interacciones_users"
    df_juegos = query_db(query_ID_JUEGO)  

    # Se filtra por los juegos que no están scrapeados
    juego_faltante = ls_urls[~ls_urls['id_juego'].isin(df_juegos['id_juego'])].reset_index()    
    
    col = ["id_usuario", "id_juego", "score", "review" ,"review_date"]

    for i in range(0, len(juego_faltante)):
        id_juego = juego_faltante['id_juego'][i] 
        
        logger.info(f'Scrapeando {id_juego}')
            
        reviews = scrape_reviews(id_juego)

        time.sleep(random.uniform(1,3))

        for j in range(0, len(reviews)):
            id_usuario, score, review, review_date = reviews[j]

            insertRow(
                "interacciones_users", #nombre de tabla
                col, #Lista de columnas
                id_usuario,
                id_juego,
                score,
                review,
                review_date
            )

            logger.info(f'{i} - {j} - {id_juego}, {id_usuario}, {score}, {review}')
    
