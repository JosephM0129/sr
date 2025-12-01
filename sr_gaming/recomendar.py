## version: 1.0 -- recomendaciones al azar
#%%
import sqlite3
import os
import random
# from surprise.dump import load
# import keras
# from keras.models import load_model
# import pickle
import pandas as pd
import numpy as np

import metricas
# from dos_torres import RankingModel

#DATABASE_FILE = os.path.dirname(os.path.abspath("__file__")) + "/datos/qll.db"
DATABASE_FILE = os.path.dirname(__file__) + "/datos/metacritics_bk.db"
ALGO_PKL = os.path.dirname(__file__) + "/datos/SVD.pkl"

DOS_TORRES = os.path.dirname(__file__) + "/datos/dt_checkpoint.0.10.keras"
MAPPING_DT = os.path.dirname(__file__) + "/datos/mappings_dt.pkl"

GRAN_TORRE = os.path.dirname(__file__) + "/datos/gt_checkpoint.0.11.keras"
MAPPING_KERAS = os.path.dirname(__file__) + "/datos/mappings.pkl"

# Ruta al archivo
PARAMS_PATH = os.path.dirname(__file__) + "/datos/svd_params.npz"


# Cargamos los parámetros una sola vez
_params = np.load(PARAMS_PATH, allow_pickle=True)

pu = _params["pu"]
qi = _params["qi"]
bu = _params["bu"]
bi = _params["bi"]
mu = float(_params["mu"])

uid_map = _params["uid_map"].item()
iid_map = _params["iid_map"].item()

def sql_execute(query, params=None):
    con = sqlite3.connect(DATABASE_FILE)
    cur = con.cursor()
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)

    con.commit()
    con.close()
    return res

def sql_select(query, params=None):
    """
    Realiza una consulta SELECT en la base de datos y devuelve una lista de registros
    cada uno de los cuales es un objeto con los nombres de los campos como atributos.
    La lista puede estar vac a si no hay registros que cumplan con la consulta

    :param query: consulta SQL a realizar
    :param params: lista de par metros a pasar a la consulta
    :return: lista de registros
    """
    con = sqlite3.connect(DATABASE_FILE)
    con.row_factory = sqlite3.Row # esto es para que devuelva registros en el fetchall
    cur = con.cursor()
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)

    # fetchall devuelve una lista de objetos, cada uno con los nombres de los campos como atributos
    ret = res.fetchall()
    con.close()
    return ret

###

def crear_usuario(id_usuario):
    query = "INSERT INTO usuarios(id_usuario) VALUES (?) ON CONFLICT DO NOTHING;" # si el id_usuario existe, se produce un conflicto y le digo que no haga nada
    sql_execute(query, [id_usuario])
    return

def insertar_interacciones(id_juego, id_usuario, score):
    query = f"INSERT INTO interacciones(id_juego, id_usuario, score) VALUES (?, ?, ?) ON CONFLICT (id_juego, id_usuario) DO UPDATE SET score=?;" # si el score existia lo actualizo
    sql_execute(query, [id_juego, id_usuario, score, score])
    return

def reset_usuario(id_usuario):
    query = f"DELETE FROM interacciones WHERE id_usuario = ?;"
    sql_execute(query, [id_usuario])
    return

def obtener_juego(id_juego):
    query = "SELECT * FROM juegos WHERE id_juego = ?;"
    juego = sql_select(query, [id_juego])[0]
    return juego

def items_valorados(id_usuario):
    """
    Devuelve una lista de ids de juegos que han sido valorados por el usuario id_usuario.
    """
    query = f"""
    SELECT id_juego FROM interacciones
    WHERE id_usuario = ? AND score > 0
    """
    rows = sql_select(query, [id_usuario])
    return [i["id_juego"] for i in rows]

def items_vistos(id_usuario):
    """
    Devuelve una lista de ids de juegos que han sido vistos por el usuario id_usuario pero no valorados.
    """
    query = f"""
    SELECT id_juego FROM interacciones
    WHERE id_usuario = ? AND score = 0
    """
    rows = sql_select(query, [id_usuario])
    return [i["id_juego"] for i in rows]

def items_desconocidos(id_usuario):
    """
    Devuelve una lista de ids de juegos que no han sido vistos ni valorados por el usuario id_usuario.
    """
    query = f"""
    SELECT id_juego FROM juegos
    WHERE id_juego NOT IN (
        SELECT id_juego FROM interacciones
        WHERE id_usuario = ? AND score IS NOT NULL
    )
    """
    rows = sql_select(query, [id_usuario])
    return [i["id_juego"] for i in rows]

def datos_juegos(id_juegos):
    query = f"SELECT DISTINCT * FROM juegos WHERE id_juego IN ({','.join(['?']*len(id_juegos))})"
    juegos = sql_select(query, id_juegos)
    return juegos

def init():
    print("init: top_juegos")
    sql_execute("DROP TABLE IF EXISTS top_juegos;")
    sql_execute("CREATE TABLE top_juegos AS SELECT id_juego, avg(score)+count(*) as avg_cant FROM interacciones GROUP BY 1;")
    
    #  TODO: usar avg(score) o avg(score)+count(*) -> se agregó para usarlo en recomendar_top_n

    print("init: pares_de_juegos")
    
    # sql_execute("DROP TABLE IF EXISTS pares_de_juegos;") --Descomentar si se quiere recalcular la tabla de pares
    sql_execute("""
                CREATE TABLE IF NOT EXISTS pares_de_juegos AS
                SELECT 
                    i1.id_juego AS id_juego_1, 
                    i2.id_juego AS id_juego_2, 
                    --count(*) AS cant,
                    --avg(i2.score) AS avg,
                    count(*) + avg(i2.score) AS avg_cant
                FROM interacciones AS i1, interacciones AS i2
                WHERE i1.id_usuario = i2.id_usuario 
                AND i1.id_juego != i2.id_juego 
                AND i1.score > 90 -- hiperparámetro
                GROUP BY 1, 2
                HAVING count(*) > 6   -- hiperparámetro
                """)
    # sql_execute("CREATE INDEX IF NOT EXISTS idx_pares_de_juegos ON pares_de_juegos (id_juego_1);")
    # TODO: usar avg(score) para los pares -> listo con count(*) + avg(i2.score)
    # TODO: optimizar hiperparámetros -> Se usa score mayor a 90 y cant mayor a 6
    return

def recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    id_juegos = random.sample(juegos_desconocidos, N)
    return id_juegos

def recomendador_top_n(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    # TODO: si tiene score bajo, lo tomo?->PROBÉ CON AVG > 90
    # TODO: en vez de cantidad de interacciones, promedio de score->se probó 0.069077

    # res = sql_select(f"SELECT id_juego FROM top_juegos WHERE id_juego NOT IN ({",".join("?"*len(juegos_relevantes))}) ORDER BY cant DESC LIMIT ?;", juegos_relevantes + [N]) # original: 0.219617
    # res = sql_select(f"SELECT id_juego FROM top_juegos WHERE id_juego NOT IN ({",".join("?"*len(juegos_relevantes))}) ORDER BY avg DESC LIMIT ?;", juegos_relevantes + [N]) # mejores score: 0.069077
    # res = sql_select(f"SELECT id_juego FROM top_juegos WHERE id_juego NOT IN ({",".join("?"*len(juegos_relevantes))}) ORDER BY avg_cant DESC LIMIT ?;", juegos_relevantes + [N]) # avg + cant: 0.222874
    placeholders = ",".join(["?"] * len(juegos_relevantes))
    query = f"""
    SELECT id_juego 
    FROM top_juegos 
    WHERE id_juego NOT IN ({placeholders}) 
    AND avg_cant > 90 
    ORDER BY avg_cant DESC 
    LIMIT ?;
    """

    res = sql_select(query, juegos_relevantes + [N + 36])  
    # Se obtiene el top N + 36 juegos para evitar que siempre se muestren los mismos juegos
    id_juegos_shuffle = random.sample(res, N)

    id_juegos = [i["id_juego"] for i in id_juegos_shuffle]

    return id_juegos

def recomendador_pares(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    # TODO:
    res = sql_select(f"""
                     SELECT DISTINCT id_juego_2 AS id_juego
                       FROM pares_de_juegos
                      WHERE id_juego_1 IN ({",".join("?"*len(juegos_relevantes))})
                        AND id_juego_2 IN ({",".join("?"*len(juegos_desconocidos))})
                      ORDER BY avg_cant DESC
                      LIMIT ?;"""
                     , juegos_relevantes+juegos_desconocidos+[N])

    id_juegos = [i["id_juego"] for i in res]

    return id_juegos[:N]

def recomendador_perfiles(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    # TODO: optimizar hiperparámetros -> listo
    # TODO: usar avg(score) -> ¿Es relevante si ya estoy filtrando por score?
    # TODO: ponderar diferentes perfiles -> distribuidor
    
    # -----------------------------------------------------------------
    # 1) Construir el perfil de géneros preferidos del usuario
    # -----------------------------------------------------------------
    res = sql_select(f"""
               SELECT l.genero, 
                      count(*) AS cant,             -- cuántos juegos de este género jugó
                      avg(score) AS avg,            -- promedio del score dado
                      count(*)+avg(score) AS avg_cant -- métrica híbrida (conteo + score medio)
                 FROM interacciones AS i 
                 JOIN juegos AS l ON i.id_juego = l.id_juego
                WHERE id_usuario = ?
                  AND i.id_juego IN ({",".join("?"*len(juegos_relevantes))})
                  AND i.score > 65                  -- hiperparámetro (filtro de calidad)
                GROUP BY 1                          -- agrupar por género
               HAVING count(*) > 5                  -- hiperparámetro: mínimo de interacciones
                ORDER BY 2 DESC;                    -- ordenar por cantidad desc
        """, [id_usuario]+juegos_relevantes)

    # Normalizar las cantidades por género para obtener un "perfil de probabilidad"
    total_genero = float(sum([i["cant"] for i in res]))
    perfil_genero = {i["genero"]: i["cant"]/total_genero for i in res}

    # -----------------------------------------------------------------
    # 2) Construir el perfil de distribuidores preferidos del usuario
    # -----------------------------------------------------------------
    res = sql_select(f"""
               SELECT l.distribuidor, 
                      count(*) AS cant,
                      avg(score) AS avg,
                      count(*)+avg(score) AS avg_cant
                 FROM interacciones AS i 
                 JOIN juegos AS l ON i.id_juego = l.id_juego
                WHERE id_usuario = ?
                  AND i.id_juego IN ({",".join("?"*len(juegos_relevantes))})
                  AND i.score > 60                  -- hiperparámetro (otro umbral)
                GROUP BY 1
               HAVING count(*) > 10                 -- hiperparámetro: mínimo de interacciones
                ORDER BY 2 DESC;
        """, [id_usuario]+juegos_relevantes)

    # Normalizar para obtener la distribución de distribuidores
    total_distribuidor = float(sum([i["cant"] for i in res]))
    perfil_distribuidor = {i["distribuidor"]: i["cant"]/total_distribuidor for i in res}

    # -----------------------------------------------------------------
    # 3) Seleccionar candidatos entre los juegos desconocidos
    #    - Se queda solo con aquellos cuyo género o distribuidor
    #      aparece en el perfil del usuario
    # -----------------------------------------------------------------
    res = sql_select(f"""
               SELECT id_juego, genero, distribuidor
                 FROM juegos
                WHERE id_juego IN ({",".join("?"*len(juegos_desconocidos))})
                  AND (
                       genero IN ({",".join("?"*len(perfil_genero.keys()))})
                       OR
                       distribuidor IN ({",".join("?"*len(perfil_distribuidor.keys()))})
                  )
          """, juegos_desconocidos
             + list(perfil_genero.keys()) 
             + list(perfil_distribuidor.keys()))

    # -----------------------------------------------------------------
    # 4) Puntuación de candidatos
    #    Cada juego se puntúa con la suma de:
    #      - peso del género (si está en perfil_genero)
    #      - peso del distribuidor (si está en perfil_distribuidor)
    # -----------------------------------------------------------------
    juegos_a_puntuar = [
        (i["id_juego"], 
         perfil_genero.get(i["genero"], 0) + 
         perfil_distribuidor.get(i["distribuidor"], 0))
        for i in res
    ]

    # Ordenar de mayor a menor puntuación
    juegos_a_puntuar = sorted(juegos_a_puntuar, key=lambda x: x[1], reverse=True)

    # Extraer solo los IDs de juegos
    id_juegos = [i[0] for i in juegos_a_puntuar]

    # -----------------------------------------------------------------
    # 5) Devolver el top N
    # -----------------------------------------------------------------
    return id_juegos[:N]

def predict_svd(raw_uid, raw_iid):
    """
    raw_uid: ID original del usuario (por ej. "user_123")
    raw_iid: ID original del item     (por ej. "movie_12")
    """

    # Si no existe user/item en entrenamiento → devolver valor neutro
    if raw_uid not in uid_map or raw_iid not in iid_map:
        return mu

    uid = uid_map[raw_uid]
    iid = iid_map[raw_iid]

    # Fórmula del SVD de Surprise
    pred = (
        mu
        + bu[uid]
        + bi[iid]
        + np.dot(pu[uid], qi[iid])
    )
    return float(pred)

def recomendador_surprise(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    '''
    Devuelve top N juegos desconocidos para el id_usuario según mayores
    estimaciones obtenidos dado el modelo ML entrenado previamente. 
    '''
    # _, algo = load(ALGO_PKL)

    juegos_a_puntuar = {}
    for id_juego in juegos_desconocidos:
        # score_predicho = algo.predict(uid=id_usuario, iid=id_juego).est
        score_predicho = predict_svd(id_usuario, id_juego)
        juegos_a_puntuar[id_juego] = score_predicho


    # Ordenar de mayor a menor puntuación
    juegos_a_puntuar = sorted(juegos_a_puntuar.items(), key=lambda x: x[1], reverse=True)

    # Extraer solo los IDs de juegos
    id_juegos = [i[0] for i in juegos_a_puntuar]

    return id_juegos[:N]

def recomendador_dos_torres(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    '''
    Devuelve top N juegos desconocidos para el id_usuario según mayores
    estimaciones obtenidos dado el modelo ML entrenado previamente. 
    '''

    # Importar el diccionario de mappings
    with open(MAPPING_DT, "rb") as f:
        mappings = pickle.load(f)
    
    user_id_2_id = mappings["user_id_2_id"]
    item_id_2_id = mappings["item_id_2_id"]

    # importar el modelo
    modelo = load_model(
        DOS_TORRES,
        custom_objects={"RankingModel": RankingModel}
    )

    # Obtiene los juegos que el usuario no ha puntuado
    con = sqlite3.connect(DATABASE_FILE)

    # Juegos NO vistos por el usuario
    df_item_ids = pd.read_sql(
        """
        SELECT id_juego 
        FROM juegos 
        WHERE id_juego NOT IN (
            SELECT id_juego 
            FROM interacciones 
            WHERE id_usuario = ?
        )
        """, 
        con, 
        params=[id_usuario]
    )

    item_ids_raw = df_item_ids["id_juego"].tolist()

    item_ids_internal = [item_id_2_id[j] for j in item_ids_raw]

    id_usuario_int = user_id_2_id[id_usuario] 
    preds = modelo.predict({
        "user_id": keras.ops.array([id_usuario_int] * len(item_ids_internal)),
        "item_id": keras.ops.array(item_ids_internal),
    }).squeeze()



    df_preds = pd.DataFrame({
        "item_id_raw": item_ids_raw,               # ID real del juego
        "item_id_int": item_ids_internal,          # ID interno
        "score": preds                      # score del modelo
    })

    df_juegos = df_preds.sort_values("score", ascending=False)

    id_juegos = [i for i in df_juegos["item_id_raw"]]

    return id_juegos[:N]

def recomendador_gran_torre(id_usuario, juegos_relevantes, juegos_desconocidos, N=9):
    '''
    Devuelve N juegos desconocidos para el id_usuario según mayores
    estimaciones obtenidos dado el modelo GRAN TORRE.
    '''
    # 1) Importar el diccionario de mappings
    with open(MAPPING_KERAS, "rb") as f:
        mappings = pickle.load(f)

    user_id_2_id          = mappings["user_id_2_id"]
    item_id_2_id          = mappings["item_id_2_id"]
    desarrollador_2_id    = mappings["desarrollador_2_id"]
    distribuidor_2_id     = mappings["distribuidor_2_id"]
    rating_2_id           = mappings["rating_2_id"]
    genero_2_id           = mappings["genero_2_id"]
    cant_reviews_min = mappings["cant_reviews_min"]
    cant_reviews_max = mappings["cant_reviews_max"]
    score_prom_min = mappings["score_prom_min"]
    score_prom_max = mappings["score_prom_max"]
    ratio_pos_min = mappings["ratio_pos_min"]
    ratio_pos_max = mappings["ratio_pos_max"]
    ratio_mix_min = mappings["ratio_mix_min"]
    ratio_mix_max = mappings["ratio_mix_max"]
    ratio_neg_min = mappings["ratio_neg_min"]
    ratio_neg_max = mappings["ratio_neg_max"]

    # para fecha
    fecha_min = mappings["fecha_min"]
    fecha_max = mappings["fecha_max"] 

    # 2) importar el modelo
    modelo = load_model(GRAN_TORRE)  

    # 3) Obtiene los juegos que el usuario no ha puntuado, es decir, desconocidos
    con = sqlite3.connect(DATABASE_FILE)
    df_cand = pd.read_sql("""
        SELECT
            j.id_juego,
            j.desarrollador,
            j.distribuidor,
            j.rating,
            j.genero,
            j.fecha_lanzamiento,
            u.cant_reviews,
            u.score_promedio,
            u.ratio_positivo,
            u.ratio_mixto,
            u.ratio_negativo
        FROM juegos j
        JOIN usuarios u ON u.id_usuario = ?
        WHERE j.id_juego NOT IN (
            SELECT id_juego
            FROM interacciones
            WHERE id_usuario = ?
        )
    """, con, params=[id_usuario, id_usuario])
    # 4) Mapping cada variable categórica y normalización

    # user_id interno (el mismo para todas las filas)
    id_usuario_int = user_id_2_id[id_usuario]
    df_cand["user_id_int"] = id_usuario_int

    # item_id interno
    df_cand["item_id_int"] = df_cand["id_juego"].map(item_id_2_id)

    # desarrollador_id interno
    df_cand["desarrollador_id_int"] = df_cand["desarrollador"].fillna( '__UNKNOWN__')
    df_cand["desarrollador_id_int"] = df_cand["desarrollador_id_int"].map(desarrollador_2_id)

    # distribuidor_id interno
    df_cand["distribuidor_id_int"] = df_cand["distribuidor"].fillna( '__UNKNOWN__')
    df_cand["distribuidor_id_int"] = df_cand["distribuidor_id_int"].map(distribuidor_2_id)

    # rating_id interno
    df_cand["rating_id_int"] = df_cand["rating"].fillna( '__UNKNOWN__')
    df_cand["rating_id_int"] = df_cand["rating_id_int"].map(rating_2_id)

    # genero_id interno
    df_cand["genero_id_int"] = df_cand["genero"].fillna( '__UNKNOWN__')
    df_cand["genero_id_int"] = df_cand["genero"].map(genero_2_id)


    ## Normalizar fecha y numéricas igual que en train
    df_cand["fecha_lanzamiento"] = pd.to_datetime(
        df_cand["fecha_lanzamiento"], errors="coerce"
    )
    df_cand["fecha_aaaamm"] = df_cand["fecha_lanzamiento"].dt.strftime("%Y%m").astype(float)

    df_cand["fecha_lanzamiento_norm"] = (
        (df_cand["fecha_aaaamm"] - fecha_min) / (fecha_max - fecha_min)
    )

    ## Normalizar variables numéricas igual que en train
    df_cand["cant_reviews_norm"] = (
        (df_cand["cant_reviews"] - cant_reviews_min)
        / (cant_reviews_max - cant_reviews_min)
    )

    df_cand["score_promedio_norm"] = (
        (df_cand["score_promedio"] - score_prom_min)
        / (score_prom_max - score_prom_min)
    )

    df_cand["ratio_positivo_norm"] = (
        (df_cand["ratio_positivo"] - ratio_pos_min)
        / (ratio_pos_max - ratio_pos_min)
    )

    df_cand["ratio_mixto_norm"] = (
        (df_cand["ratio_mixto"] - ratio_mix_min)
        / (ratio_mix_max - ratio_mix_min)
    )

    df_cand["ratio_negativo_norm"] = (
        (df_cand["ratio_negativo"] - ratio_neg_min)
        / (ratio_neg_max - ratio_neg_min)
    )


    # 5) Armar input_dict AL ESTILO "lista de items" 

    input_dict = {
            "user_id":        df_cand["user_id_int"].to_numpy(dtype="float32"),
            "item_id":        df_cand["item_id_int"].to_numpy(dtype="float32"),
            "desarrollador_id": df_cand["desarrollador_id_int"].to_numpy(dtype="float32"),
            "distribuidor_id":  df_cand["distribuidor_id_int"].to_numpy(dtype="float32"),
            "rating_id":        df_cand["rating_id_int"].to_numpy(dtype="float32"),
            "genero_id":        df_cand["genero_id_int"].to_numpy(dtype="float32"),
            "fecha_lanzamiento": df_cand["fecha_lanzamiento_norm"].to_numpy(dtype="float32"),
            "cant_reviews":      df_cand["cant_reviews_norm"].to_numpy(dtype="float32"),
            "score_promedio":    df_cand["score_promedio_norm"].to_numpy(dtype="float32"),
            "ratio_positivo":    df_cand["ratio_positivo_norm"].to_numpy(dtype="float32"),
            "ratio_mixto":       df_cand["ratio_mixto_norm"].to_numpy(dtype="float32"),
            "ratio_negativo":    df_cand["ratio_negativo_norm"].to_numpy(dtype="float32"),
        }

    # 6) Predecir para TODOS los juegos candidatos 

    preds = modelo.predict(input_dict, batch_size=1024).squeeze()
    df_cand["pred_score"] = preds

    # 7) Extraer Top N recomendaciones
    df_juegos = df_cand.sort_values("pred_score", ascending=False) 
    id_juegos = [i for i in df_juegos["id_juego"]]

    return id_juegos[:N]


def recomendar(id_usuario, juegos_relevantes=None, juegos_desconocidos=None, N=9):
    """
    Recomendador genrico que devuelve una lista de N juegos desconocidos
    para el usuario id_usuario. Si se proporcionan juegos_relevantes o
    juegos_desconocidos, se utilizan para filtrar la lista de juegos que
    se recomiendan. De lo contrario, se utilizan las listas de juegos
    valorados y desconocidos para el usuario.

    Te doy 9 juegos no vistos y no valorados al azar.
    """
    if not juegos_relevantes:
        # si no se proporcionan juegos relevantes, se utilizan los juegos valorados
        juegos_relevantes = items_valorados(id_usuario)

    if not juegos_desconocidos:
        # si no se proporcionan juegos desconocidos, se utilizan los juegos no vistos y no valorados
        juegos_desconocidos = items_desconocidos(id_usuario)

    # se devuelve una lista de N juegos desconocidos que se recomiendan
    # return recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N)

    if len(juegos_relevantes) <= 50: # TODO: cambiar este límite -> 50 REPRESENTA el 10% de la cantidad de juegos valorados por usuario
        rec = recomendador_top_n(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N)
    elif 50 < len(juegos_relevantes) <= 300: # TODO: cambiar este límite -> 300 represetna el 50%
        rec = recomendador_pares(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_surprise(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec =  recomendador_dos_torres(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_perfiles(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_gran_torre(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_top_n(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N) 
    else:
        rec = recomendador_surprise(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec =  recomendador_dos_torres(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_pares(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_top_n(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_gran_torre(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        # rec = recomendador_perfiles(id_usuario, juegos_relevantes, juegos_desconocidos, N)
        #Comentar recomendador_perfiles y probar con surprise. Entrenar con todos los juegos. 

    return rec
def recomendar_contexto(id_usuario, id_juego, juegos_relevantes=None, juegos_desconocidos=None, N=3):
    """
    Recomendador de contexto que devuelve una lista de N juegos desconocidos
    para el usuario id_usuario, dados los juegos relevantes y desconocidos.

    Si no se proporcionan juegos relevantes, se utilizan los juegos valorados
    para el usuario.

    Si no se proporcionan juegos desconocidos, se utilizan los juegos no vistos
    y no valorados para el usuario.

    La recomendacion se hace al azar.

    :param id_usuario: id del usuario al que se le va a recomendar
    :param id_juego: id del juego que se esta mirando
    :param juegos_relevantes: lista de ids de juegos relevantes para el usuario
    :param juegos_desconocidos: lista de ids de juegos desconocidos para el usuario
    :param N: numero de recomendaciones que se quieren hacer
    :return: lista de ids de juegos desconocidos recomendados
    """
    if not juegos_relevantes:
        # si no se proporcionan juegos relevantes, se utilizan los juegos valorados
        juegos_relevantes = items_valorados(id_usuario)

    if not juegos_desconocidos:
        # si no se proporcionan juegos desconocidos, se utilizan los juegos no vistos y no valorados
        juegos_desconocidos = items_desconocidos(id_usuario)

    # se devuelve una lista de N juegos desconocidos que se recomiendan
    # return recomendador_azar(id_usuario, juegos_relevantes, juegos_desconocidos, N)

    return recomendador_top_n(id_usuario, juegos_relevantes, juegos_desconocidos, N)

###

if __name__ == '__main__':
    def test(id_usuario):
        """
        Function to test the recomendation algorithm with a given user.

        Le da score dado un usuario.

        Parameters
        ----------
        id_usuario : int
            The id of the user to test the recomendation algorithm.

        Returns
        -------
        score : float
            The score of the recomendation algorithm.
        """
        # Get the list of relevant and non relevant games for the user
        juegos_relevantes = items_valorados(id_usuario)
        juegos_desconocidos = items_vistos(id_usuario) + items_desconocidos(id_usuario)

        # Shuffle the list of relevant games
        random.shuffle(juegos_relevantes)

        # Split the list of relevant games into training and testing sets
        corte = int(len(juegos_relevantes)*0.8)
        juegos_relevantes_training = juegos_relevantes[:corte]
        juegos_relevantes_testing = juegos_relevantes[corte:] + juegos_desconocidos

        # Get the recomendation for the user
        # La idea es que las primeras recomendaciones son los que esté en el testing y no los desconocidos.
        recomendacion = recomendar(id_usuario, juegos_relevantes_training, juegos_relevantes_testing, 9)
        # Calculate the relevance scores for the recomendation
        relevance_scores = []
        for id_juego in recomendacion:
            
            # Get the score of the game from the database
            res = sql_select("SELECT score FROM interacciones WHERE id_usuario = ? AND id_juego = ?;", [id_usuario, id_juego])
            if res is not None and len(res) > 0:
                score = res[0][0]
            else:
                score = 0

            # Append the score to the list of relevance scores
            relevance_scores.append(score)

        # Calculate the normalized discounted cumulative gain of the recomendation
        score = metricas.normalized_discounted_cumulative_gain(relevance_scores)

        return score

    init()

    # Obtiene la lista de usuarios con mas de 100 interacciones (más influyentes)
    id_usuarios = sql_select("SELECT id_usuario FROM usuarios WHERE (SELECT count(*) FROM interacciones WHERE id_usuario = usuarios.id_usuario) >= 100 limit 50;")
    
    # Obtiene el id de cada usuario
    id_usuarios = [i["id_usuario"] for i in id_usuarios]

    # Se calcula NDCG de cada usuario y se promedia.
    scores = []
    for id_usuario in id_usuarios:
        score = test(id_usuario)
        scores.append(score)
        print(f"{id_usuario} >> {score:.6f}")

    print(f"NDCG: {sum(scores)/len(scores):.6f}")
