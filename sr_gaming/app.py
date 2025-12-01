from flask import Flask, request, render_template, make_response, redirect
import recomendar

app = Flask(__name__)
app.debug = True

@app.get('/')
def get_index():
    return render_template('login.html')

@app.post('/')
def post_index():
    """ 
    Procesa el formulario de login y redirige al usuario a la
    página de recomendaciones si se proporciona un id_usuario.
    Si no, muestra el formulario de login de nuevo.
    """
    # obtengo el id_usuario ingresado en el formulario de login
    # Abre la base de datos "usuarios"  y agarra el id_usuario
    id_usuario = request.form.get('id_usuario', None)

    if id_usuario: # si me mandaron el id_usuario
        #Si no existe, lo crea. Si existe no hace nada.
        recomendar.crear_usuario(id_usuario)

        # mando al usuario a la página de recomendaciones
        res = make_response(redirect("/recomendaciones"))

        # pongo el id_usuario en una cookie para recordarlo
        res.set_cookie('id_usuario', id_usuario)
        return res

    # sino, le muestro el formulario de login
    return render_template('login.html')

@app.get('/recomendaciones')
def get_recomendaciones():
    id_usuario = request.cookies.get('id_usuario')

    id_juegos = recomendar.recomendar(id_usuario)

    # pongo juegos vistos con score = 0
    for id_juego in id_juegos:
        recomendar.insertar_interacciones(id_juego, id_usuario, 0)

    juegos_recomendados = recomendar.datos_juegos(id_juegos)
    cant_valorados = len(recomendar.items_valorados(id_usuario))
    cant_vistos = len(recomendar.items_vistos(id_usuario))

    juegos_relevantes = recomendar.items_valorados(id_usuario)

    if len(juegos_relevantes) <= 50: 
        rec = 'TOP N'
    elif 50 < len(juegos_relevantes) <= 300: 
        rec = 'PARES'
    else:
        rec = 'SVD'

    return render_template("recomendaciones.html", juegos_recomendados=juegos_recomendados, id_usuario=id_usuario, cant_valorados=cant_valorados, cant_vistos=cant_vistos, rec=rec)

@app.get('/recomendaciones/<string:id_juego>') #Saca el id de juego que sea stirng y se la pasa a la funcion
def get_recomendaciones_juego(id_juego):
    id_usuario = request.cookies.get('id_usuario')

    # Se relaciona a lo que está viendo (contexto)
    id_juegos = recomendar.recomendar_contexto(id_usuario, id_juego)

    # pongo juegos vistos con score = 0
    for i in id_juegos:
        recomendar.insertar_interacciones(i, id_usuario, 0)

    juegos_recomendados = recomendar.datos_juegos(id_juegos)
    cant_valorados = len(recomendar.items_valorados(id_usuario))
    cant_vistos = len(recomendar.items_vistos(id_usuario))

    # Es el libro que estoy mirando
    juego = recomendar.obtener_juego(id_juego)

    juegos_relevantes = recomendar.items_valorados(id_usuario)

    if len(juegos_relevantes) <= 50: 
        rec = 'TOP N'
    elif 50 < len(juegos_relevantes) <= 300: 
        rec = 'PARES'
    else:
        rec = 'SVD'    

    return render_template("recomendaciones_juego.html", juego=juego, juegos_recomendados=juegos_recomendados, id_usuario=id_usuario, cant_valorados=cant_valorados, cant_vistos=cant_vistos, rec=rec)


@app.post('/recomendaciones')
def post_recomendaciones():
    id_usuario = request.cookies.get('id_usuario')

    # inserto los ratings enviados como interacciones
    for id_juego in request.form.keys():
        score = int(request.form[id_juego])
        if score > 0: # 0 es que no puntuó
            recomendar.insertar_interacciones(id_juego, id_usuario, score)

    return make_response(redirect("/recomendaciones"))

@app.get('/reset')
def get_reset():
    id_usuario = request.cookies.get('id_usuario')
    recomendar.reset_usuario(id_usuario)

    return make_response(redirect("/recomendaciones"))

if __name__ == '__main__':
    app.run()


