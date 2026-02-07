import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = 'clave_secreta_para_sesion' # Necesario para mostrar mensajes flash

# Configuración de carpeta de subida
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'data_source.xlsx')
DB_NAME = 'aduana.db'

@app.route('/', methods=['GET', 'POST'])
def index():
    resultado = None
    archivo_cargado = os.path.exists(DB_NAME)
    
    # Lógica de búsqueda
    if request.method == 'POST' and 'search_action' in request.form:
        if not archivo_cargado:
            flash('Primero debes cargar un archivo de datos.', 'warning')
        else:
            posicion = request.form.get('posicion', '').strip()
            mercaderia = request.form.get('mercaderia', '').strip()
            importador = request.form.get('importador', '').strip()
            resultado = procesar_datos(posicion, mercaderia, importador)

    return render_template('index.html', resultado=resultado, archivo_cargado=archivo_cargado)

@app.route('/sugerencias')
def sugerencias():
    query = request.args.get('q', '')
    
    try:
        engine = create_engine(f'sqlite:///{DB_NAME}')
        # Buscamos las primeras 10 coincidencias distintas
        sql = text('SELECT DISTINCT "POSICION ARANCELARIA" FROM importaciones WHERE "POSICION ARANCELARIA" LIKE :q LIMIT 10')
        df = pd.read_sql(sql, engine, params={'q': f'%{query}%'})
        return jsonify(df['POSICION ARANCELARIA'].tolist())
    except Exception:
        return jsonify([])

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No se seleccionó ningún archivo', 'danger')
        return redirect(url_for('index'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('Nombre de archivo vacío', 'danger')
        return redirect(url_for('index'))

    if file:
        # Guardamos el archivo siempre con el mismo nombre para reemplazar el anterior
        file.save(FILE_PATH)
        
        try:
            # Procesar y guardar en SQL
            df = pd.read_excel(FILE_PATH)
            df.columns = [c.strip() for c in df.columns]
            
            if 'POSICION ARANCELARIA' not in df.columns or 'FOB DOLAR' not in df.columns:
                flash('El archivo no tiene las columnas "POSICION ARANCELARIA" o "FOB DOLAR".', 'danger')
                return redirect(url_for('index'))

            # Limpieza y conversión antes de guardar
            df['POSICION ARANCELARIA'] = df['POSICION ARANCELARIA'].astype(str).str.strip()
            df['FOB DOLAR'] = pd.to_numeric(df['FOB DOLAR'], errors='coerce')
            
            engine = create_engine(f'sqlite:///{DB_NAME}')
            df.to_sql('importaciones', con=engine, if_exists='replace', index=False)
            
            flash('Archivo importado y guardado en SQL exitosamente. Ahora puedes realizar búsquedas.', 'success')
        except Exception as e:
            flash(f'Error procesando el archivo: {str(e)}', 'danger')
            
        return redirect(url_for('index'))

def procesar_datos(posicion, mercaderia, importador):
    try:
        engine = create_engine(f'sqlite:///{DB_NAME}')
        
        # Construcción dinámica de filtros
        conditions = []
        params = {}

        if posicion:
            conditions.append('"POSICION ARANCELARIA" LIKE :pos')
            params['pos'] = f'%{posicion}%'
        
        if mercaderia:
            conditions.append('"MERCADERIA" LIKE :merc')
            params['merc'] = f'%{mercaderia}%'
            
        if importador:
            conditions.append('"IMPORTADOR" LIKE :imp')
            params['imp'] = f'%{importador}%'
            
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Columnas que queremos mostrar en la tabla
        cols_select = '"POSICION ARANCELARIA", "DESPACHO", "ITEM", "FOB DOLAR", "MERCADERIA", "CANTIDAD", "OFICIALIZACION"'
        
        # Consulta para el MÁXIMO valor
        query_max = text(f'SELECT {cols_select} FROM importaciones WHERE {where_clause} ORDER BY "FOB DOLAR" DESC LIMIT 1')
        df_max = pd.read_sql(query_max, engine, params=params)

        # Consulta para el MÍNIMO valor
        query_min = text(f'SELECT {cols_select} FROM importaciones WHERE {where_clause} ORDER BY "FOB DOLAR" ASC LIMIT 1')
        df_min = pd.read_sql(query_min, engine, params=params)
        
        if df_max.empty or df_min.empty:
            return {'error': 'No se encontraron registros con los filtros proporcionados.'}

        # Convertimos los resultados a diccionarios (records)
        rec_max = df_max.iloc[0].to_dict()
        rec_min = df_min.iloc[0].to_dict()

        # Formateamos el dinero para que se vea bonito
        rec_max['FOB_FMT'] = f"${rec_max['FOB DOLAR']:,.2f}"
        rec_min['FOB_FMT'] = f"${rec_min['FOB DOLAR']:,.2f}"
    
        # Formateamos Cantidad (sin decimales)
        rec_max['CANTIDAD'] = f"{rec_max['CANTIDAD']:,.0f}"
        rec_min['CANTIDAD'] = f"{rec_min['CANTIDAD']:,.0f}"

        # Formateamos Fecha (solo fecha, sin hora)
        if pd.notna(rec_max['OFICIALIZACION']):
            rec_max['OFICIALIZACION'] = pd.to_datetime(rec_max['OFICIALIZACION']).strftime('%d/%m/%Y')
        if pd.notna(rec_min['OFICIALIZACION']):
            rec_min['OFICIALIZACION'] = pd.to_datetime(rec_min['OFICIALIZACION']).strftime('%d/%m/%Y')

        return {
            'max_record': rec_max,
            'min_record': rec_min
        }

    except Exception as e:
        return {'error': f'Ocurrió un error procesando el archivo: {str(e)}'}

if __name__ == '__main__':
    app.run(debug=True)
