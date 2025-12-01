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
import logging

from create_db import insertRow,query_db

# 1) logger a nivel de módulo
logger = logging.getLogger(__name__)

# Configuramos el nivel de logging
logging.basicConfig(level=logging.WARNING)

def scrap_game_details(url):
    """
    Scrapea la página de un juego de Metacritic, extrae los detalles y devuelve
    una tupla con los siguientes valores:
        - id_juego: identificador del juego (parte final de la URL)
        - descripcion: descripción del juego
        - plataforma: lista de plataformas (strings)
        - fecha_sql: fecha de lanzamiento en formato "YYYY-MM-DD"
        - desarrollador: lista de desarrolladores (strings)
        - distribuidor: distribuidor del juego (string)
        - genero: lista de géneros (strings)
    """

    headers = {
        "User-Agent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/15.0 Mobile/15E148 Safari/604.1"),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    id_juego = os.path.basename(url)

    url_game_details = url + '/details'

    # Hacemos una request a la URL y parseamos el HTML
    resp = requests.get(url_game_details, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Extraemos el titulo de juego
    titulo = soup.find("a", class_="c-productSubpageHeader_back").get_text(strip=True)

    # Extramos la descripción del juego
    descripcion = soup.find("div", 
            class_="c-pageProductDetails_description g-outer-spacing-bottom-xlarge"
            ).get_text(strip=True).replace("Description:", "")

    # Extramos las plataformas en una lista
    plataformas = soup.find_all("li", 
            class_="c-gameDetails_listItem g-color-gray70 u-inline-block"
            )
    plataforma = {pltf.get_text(strip=True) for pltf in plataformas}
    plataforma = ', '.join(list(plataforma))

    # Extramos la fecha de lanzamiento y la parseamos a formato SQL
    fch_lanzamiento = soup.find("span", 
            class_="g-outer-spacing-left-medium-fluid g-color-gray70 u-block"
            )

    if fch_lanzamiento:
        fecha_obj = datetime.strptime(fch_lanzamiento.get_text(strip=True), "%b %d, %Y")   # parsea string → datetime
        fecha_sql = fecha_obj.strftime("%Y-%m-%d") 
    elif fch_lanzamiento is None:
        fecha_sql = None 
        logger.warning("Fecha de lanzamiento no encontrada")

    # Extramos los desarrolladores en una lista
    desarrollador = soup.find_all("li", 
            class_=[
            "c-gameDetails_listItem u-inline-block g-color-gray70",
            "c-gameDetails_listItem u-inline-block"   
            ]
            )

    desarrollador = {devep.get_text(strip=True) for devep in desarrollador}
    desarrollador = ', '.join(list(desarrollador))

    # Extramos el distribuidor
    distribuidor = soup.find_all(["a", "span"], 
            class_=[   
            "g-outer-spacing-left-medium-fluid u-block u-text-underline",
            "g-outer-spacing-left-medium-fluid u-block g-color-gray70",
            ]
            )

    distribuidor = {distri.get_text(strip=True) for distri in distribuidor}
    distribuidor = ', '.join(list(distribuidor))

    # Extramos los géneros en una lista
    genero = soup.find_all("li", 
            class_="c-genreList_item"
            )
    genero = {genr.get_text(strip=True) for genr in genero}
    genero = ','.join(list(genero))

    return id_juego, titulo, descripcion, plataforma, fecha_sql, desarrollador, distribuidor, genero, url_game_details


if __name__ == "__main__":

    # Se agrega el nombre de juego al dataframe
    ls_urls = pd.read_csv("data/urls_metacritic.csv", sep="|")
    ls_urls["id_juego"] = ls_urls["url"].apply(os.path.basename)

    # Se obtienen los juegos de la base de datos
    query_ID_JUEGO = "SELECT id_juego FROM JUEGOS"
    df_juegos = query_db(query_ID_JUEGO)  

    # Se filtra por los juegos que no están scrapeados
    url_faltante = ls_urls[~ls_urls['id_juego'].isin(df_juegos['id_juego'])].reset_index()

    inicio = 0
    fin = len(url_faltante)-inicio

    col = ["id_juego", "titulo", "descripcion", "plataforma", "fecha_lanzamiento", "desarrollador", "distribuidor", "genero", "url_details"]

    # Loop sobre los juegos que faltan 
    for i in range(inicio, fin):
        id_juego, titulo, descripcion, plataforma, fecha_lanzamiento, desarrollador, distribuidor, genero, url_details = scrap_game_details(url_faltante['url'][i])
       
        print(f'--------{i}--------')
        insertRow(
            "juegos", #nombre de tabla
            col, #Lista de columnas
            id_juego,
            titulo,
            descripcion,
            plataforma,
            fecha_lanzamiento,
            desarrollador,
            distribuidor,
            genero,
            url_details
        )
        print(f'--------{i}--------')

        time.sleep(0.5)
            


