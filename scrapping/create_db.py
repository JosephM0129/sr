#%%
from pathlib import Path
import sqlite3 as sql
import pandas as pd

#%%
# db = "metacritics-test.db"
db = "metacritics.db"
path_db = Path.cwd() / "data" / db


def create_db():
    """
    Crea la base de datos y sus tablas para almacenar los datos de los lectores
    y sus interacciones.
    """
    if not path_db.is_file():
        con = sql.connect(f'./data/{db}')
        con.commit()
        con.close()
        print('Se crea la base de datos')
    else:
        print('La base de datos ya existe')

def create_tables():
    """
    Crea las tablas de la base de datos:
    - juegos: con la informacion de cada juego.
    """
    con = sql.connect(f'./data/{db}')
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS juegos (
            id_juego TEXT PRIMARY KEY,  
            titulo TEXT,
            rating TEXT,
            descripcion TEXT,  
            plataforma TEXT,  
            fecha_lanzamiento DATE,  
            desarrollador TEXT,  
            distribuidor TEXT,  
            genero TEXT,  
            img_url TEXT,
            url_details TEXT
            
        ) 
        """
    )

    cur.execute("""
        CREATE TABLE IF NOT EXISTS juegos_detalle (
            id_juego TEXT PRIMARY KEY,  
            num_page INT,
            rating TEXT,
            img_url TEXT
        ) 
        """
    )       

    cur.execute("""
        CREATE TABLE IF NOT EXISTS interacciones (
            id_usuario TEXT,
            id_juego TEXT,  
            score int,
            review TEXT,
            review_date DATE,
            PRIMARY KEY (id_usuario, id_juego)
        ) 
        """
    ) 

    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id_usuario TEXT PRIMARY KEY,
            score_promedio int,
            cant_reviews int,
            ratio_positivo int,
            ratio_mixto int,
            ratio_negativo int
        ) 
        """
    )    

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id_usuario TEXT PRIMARY KEY,
            score_promedio int,
            cant_reviews int,
            ratio_positivo int,
            ratio_mixto int,
            ratio_negativo int
        ) 
        """
    )       
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interacciones_users (
            id_usuario TEXT,
            id_juego TEXT,  
            score int,
            review TEXT,
            review_date DATE,
            PRIMARY KEY (id_usuario, id_juego)
        ) 
        """
    )
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuario_no_encontrado (
            id_usuario TEXT,
            PRIMARY KEY (id_usuario)
        ) 
        """        
    )   

    con.commit()
    con.close()

def insertRow(tabla, columnas, *row):
    """
    Inserta una fila en la tabla indicada

    Ejemplo:
    insertRow("juegos", ["id", "nombre", "fecha_lanzamiento"], 1, "BioShock", "2007-08-21")

    """
    print(f"Insertando en la tabla {tabla}")
    col_names = ','.join(columnas)
    placeholders = ','.join(['?'] * len(row))
    instruccion = f"INSERT INTO {tabla} ({col_names}) VALUES ({placeholders})"

    print(f"Instruccion: {instruccion}")
    print(f"Valores: {row}")

    with sql.connect(f'./data/{db}') as con:
        cur = con.cursor()

        try:
            # código que intenta insertar el registro
            cur.execute(instruccion, row)
            con.commit()
            print('Registro insertado correctamente.')
        except sql.IntegrityError as e:
            # código que se ejecuta si se produce el error
            if "UNIQUE constraint failed:" in str(e):
                print(f"El id {row[0]} ya existe en la base de datos")
            else:
                print(f"Error inesperado: {e}")   

def query_db(query):
    """
    Ejecuta una consulta SQL en la base de datos de metacritic
    y devuelve un DataFrame con los resultados.

    Args:
        query (str): Consulta SQL que se va a ejecutar.

    Returns:
        pd.DataFrame: Resultados de la consulta SQL.
    """
    with sql.connect(f'./data/{db}') as con:
        cur = con.cursor()    
        df = pd.read_sql_query(query, con)
    
    return df  

def update():

    query_update1 = '''
        update interacciones  
        set id_usuario ='d%2Bpad-magazine'
        where id_usuario = 'dpad-magazine'
    '''

    query_update2 = '''
        UPDATE juegos AS a
        SET 
            rating = b.rating,
            img_url = b.img_url
        FROM juegos_detalle AS b
        WHERE a.id_juego = b.id_juego;
    '''   

    with sql.connect(f'./data/{db}') as con:
        cur = con.cursor()

        cur.execute(query_update1)
        cur.execute(query_update2)
        con.commit()
        print('Registros actualizados.')




if __name__ == "__main__":
    create_db()
    create_tables()
    update()

'''
delete FROM interacciones  
where score = 'tbd';

delete FROM usuarios where id_usuario in (

select id_usuario from (
select
	i.id_usuario,
	count(*) as cant_interac
from interacciones i 
group by 1
having count(*) < 20
)
);

delete FROM interacciones  where id_usuario not in (


select
	id_usuario
from usuarios  
);


delete FROM juegos  where id_juego not in (
select
	i.id_juego
from interacciones i  
)


'''
