#%%
import os
import time
import csv
import shutil
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import logging
import re
import html

from create_db import insertRow,query_db

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

def img_url_real(card):
    img_tag = card.find("picture", class_ = 'c-cmsImage c-cmsImage-loaded')

    if img_tag: 
        img_tag = img_tag.find("img", src=True) 
    else:
        return None
    # 1) elegir atributo correcto
    src = (img_tag.get("src")
           or img_tag.get("data-src")
           or img_tag.get("data-image"))
    if not src:
        srcset = img_tag.get("srcset")
        if srcset:
            # tomar el primer candidato
            src = srcset.split(",")[0].strip().split(" ")[0]
    if not src:
        return None

    # 2) desescapar &amp; → &
    src = html.unescape(src)

    # 3) absolutizar
    return src


def get_soup(reviews_url):

    driver = webdriver.Chrome(service=Service("./chromedriver-win64/chromedriver.exe"), options=options)
    driver.set_page_load_timeout(120)
    # driver.get(reviews_url)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(reviews_url)
    except TimeoutException:
        # si igual se pasa, cortamos la carga y seguimos con lo que haya
        driver.execute_script("window.stop();") 

     # --- Nuevo: scrolleo simple ---
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(5):  # probá con 5–10 scrolls, ajustá según necesidad
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)  # darle tiempo a la página para cargar más reseñas
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # si no creció, no hay más contenido
            break
        last_height = new_height

    # --- Extraer HTML ya cargado ---
    page_content = driver.page_source
    driver.quit()

    soup = BeautifulSoup(page_content, "lxml")

    return soup

def get_game_details(card):
    
    # 1) URL del juego (href del <a>)
    a = card.find("a", href=True)
    game_url = a["href"] if a else None
    id_juego = os.path.basename(game_url.strip("/"))


    # 2) Rated (dentro de .c-finderProductCard_meta)
    rated = None
    meta = card.select_one(".c-finderProductCard_meta")
    if meta:
        # estructura típica: <span><span class="u-text-capitalize">Rated</span> E</span>
        rated_label = meta.select_one("span .u-text-capitalize")
        if rated_label and rated_label.parent:
            txt = rated_label.parent.get_text(" ", strip=True)   # "Rated E"
            rated = txt.replace("Rated", "").strip()

    img_url = img_url_real(card)

    return id_juego, rated, img_url

if __name__ == "__main__":

    # Se obtienen los juegos de la base de datos
    query_ult_pag= "SELECT max(num_page) as max FROM juegos_detalle"
    max_num_page = query_db(query_ult_pag) 

    inicio_page = 1 if max_num_page['max'][0] is None else max_num_page['max'][0]
    fin_page = 578

    col = ["id_juego", "num_page", "rating", "img_url"]

    for page in range(inicio_page, fin_page+1):
        START_URL = f"https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2025&page={page}"
        
        logger.info(f"Scrapeando página {START_URL}")
        soup = get_soup(START_URL)
        cards = soup.find_all("div", class_="c-finderProductCard c-finderProductCard-game")
        for card in cards:
            id_juego, rated, img_url = get_game_details(card)
            insertRow(
                "juegos_detalle", #nombre de tabla
                col, #Lista de columnas
                id_juego, page, rated, img_url
            )
            logger.info(f'{page} - {id_juego}, {rated}, {img_url}')
        

