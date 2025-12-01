#%%
import os
import time
import csv
import shutil
import random
from datetime import datetime
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import re

from create_db import insertRow,query_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.metacritic.com/user"
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

def dig_dentro_parentesis(texto):
    """
    Extrae el valor numérico entre paréntesis de un string.
    
    Ejemplo:
    >>> dig_dentro_parentesis("(123)")
    '123'
    """
    # Buscamos el patrón: "(<números>)"
    # y capturamos solo los números dentro de "("
    m = re.search(r"\((\d+)", texto)
    
    # Si se encontró el patrón, devolvemos el grupo 1 (los números)
    return m.group(1)


def scrap_user_details(id_usuario):
    """
    Scrapea la página de un usuario de Metacritic, extrae los detalles y devuelve
    una tupla con los siguientes valores:
        - id_usuario: identificador del usuario
        - avg_score: promedio de score del usuario
        - cant_reviews: cantidad de reviews del usuario
        - ratio_pos: porcentaje de reviews positivas
        - ratio_mix: porcentaje de reviews mixtas
        - ratio_neg: porcentaje de reviews negativas
    """
    reviews_url = f'{BASE_URL}/{id_usuario}/?filter=games'

    logger.info(f'Scrapeando {reviews_url}')

    # Hacemos una request a la URL y parseamos el HTML
    resp = requests.get(reviews_url, headers=headers)
    
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Extramos la descripción del juego
    avg_score = soup.find("span", class_="c-scoreOverview_avgScoreText")
    if avg_score:
        avg_score = avg_score.get_text(strip=True)
    else:
        avg_score = None 
        logger.warning("avg_score no encontrada")
    
    # Extramos la cantidad de reviews
    cant_reviews = soup.find("span", class_="c-globalHeader_menu_subText").get_text(strip=True)

    # Extramos los porcentajes de reviews
    scoreCount = soup.find_all("div", class_="c-scoreCount_count")
    if scoreCount:
        # Extraemos el porcentaje de reviews positivas
        ratio_pos = dig_dentro_parentesis(scoreCount[0].get_text(strip=True))
        # Extraemos el porcentaje de reviews mixtas
        ratio_mix = dig_dentro_parentesis(scoreCount[1].get_text(strip=True))
        # Extraemos el porcentaje de reviews negativas
        ratio_neg = dig_dentro_parentesis(scoreCount[2].get_text(strip=True))
    else:
        ratio_pos = None 
        ratio_mix = None
        ratio_neg = None
        logger.warning("scoreCount no encontrada")    



    return id_usuario, avg_score, cant_reviews, ratio_pos, ratio_mix, ratio_neg

if __name__ == "__main__":

    # Se obtienen los juegos de la base de datos
    query_usuario_usuario = "SELECT id_usuario FROM users"
    df_usuario_usuario = query_db(query_usuario_usuario)  

    # Se obtienen los usuarios unicios de la base de datos
    query_usuario_interacciones = "SELECT distinct id_usuario FROM interacciones_users"
    df_usuario_interacciones = query_db(query_usuario_interacciones)     

    # Se filtra por los usuarios que no están scrapeados
    usuario_faltante = df_usuario_interacciones[~df_usuario_interacciones['id_usuario'].isin(df_usuario_usuario['id_usuario'])].reset_index() 

    col = ["id_usuario", "score_promedio", "cant_reviews", "ratio_positivo", "ratio_mixto", "ratio_negativo"]
    # Loop sobre los juegos que faltan 
    for i in range(0, len(usuario_faltante)):
        id_usuario = usuario_faltante['id_usuario'][i] 
        
        try:
            id_usuario, score_promedio, cant_reviews, ratio_positivo, ratio_mixto, ratio_negativo = scrap_user_details(id_usuario)
        except requests.exceptions.HTTPError as e:   
            code = getattr(e, 'response').status_code
            
            if code == 404:
                logger.warning(f'{id_usuario} no encontrado')
                
                insertRow(
                    "usuario_no_encontrado", #nombre de tabla
                    ['id_usuario'], #Lista de columnas
                    id_usuario,
                )
                continue
            raise
       
        insertRow(
            "users", #nombre de tabla
            col, #Lista de columnas
            id_usuario,
            score_promedio,
            cant_reviews,
            ratio_positivo,
            ratio_mixto,
            ratio_negativo
        )

        time.sleep(random.uniform(1,3))

        logger.info(f'{i}, {id_usuario}, {score_promedio}, {cant_reviews}, {ratio_positivo}, {ratio_mixto}, {ratio_negativo}')
            


