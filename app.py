import os
import io
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = 'clave_secreta_para_sesion' # Necesario para mostrar mensajes flash

# Configuración de carpeta de subida
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
FILE_PATH = os.path.join(app.config['UPLOAD_FOLDER'], 'data_source.xlsx')
DB_NAME = 'datos.db'

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
    campo = request.args.get('campo', 'posicion')
    
    try:
        engine = create_engine(f'sqlite:///{DB_NAME}')
        
        columna = 'IMPORTADOR' if campo == 'importador' else 'POSICION ARANCELARIA'
        
        # Buscamos las primeras 10 coincidencias distintas
        sql = text(f'SELECT DISTINCT "{columna}" FROM importaciones WHERE "{columna}" LIKE :q LIMIT 10')
        df = pd.read_sql(sql, engine, params={'q': f'%{query}%'})
        return jsonify(df[columna].tolist())
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
            
            # Convertir nuevas columnas numéricas si existen
            for col in ['VALOR LISTA', 'VALOR PLANILLA', 'VALOR RES']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Inicializar columna OBSERVACION si no existe
            if 'OBSERVACION' not in df.columns:
                df['OBSERVACION'] = ''

            engine = create_engine(f'sqlite:///{DB_NAME}')
            df.to_sql('importaciones', con=engine, if_exists='replace', index=False)
            
            flash('Archivo importado y guardado en SQL exitosamente. Ahora puedes realizar búsquedas.', 'success')
        except Exception as e:
            flash(f'Error procesando el archivo: {str(e)}', 'danger')
            
        return redirect(url_for('index'))

@app.route('/update_observation', methods=['POST'])
def update_observation():
    try:
        data = request.get_json()
        despacho = data.get('despacho')
        item = data.get('item')
        observacion = data.get('observacion')

        engine = create_engine(f'sqlite:///{DB_NAME}')
        with engine.connect() as conn:
            # Usamos DESPACHO e ITEM como identificadores únicos del registro
            sql = text('UPDATE importaciones SET OBSERVACION = :obs WHERE DESPACHO = :desp AND ITEM = :item')
            conn.execute(sql, {'obs': observacion, 'desp': despacho, 'item': item})
            conn.commit()
            
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/export')
def export_file():
    try:
        engine = create_engine(f'sqlite:///{DB_NAME}')
        df = pd.read_sql('SELECT * FROM importaciones', engine)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Datos')
        output.seek(0)
        
        return send_file(output, download_name='datos_exportados.xlsx', as_attachment=True)
    except Exception as e:
        flash(f'Error al exportar: {str(e)}', 'danger')
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
        cols_select = '*'
        
        # Expresión SQL para calcular el Valor Unitario (FOB / CANTIDAD) manejando ceros
        sql_unit_val = 'CASE WHEN "CANTIDAD" > 0 THEN "FOB DOLAR" / "CANTIDAD" ELSE 0 END'

        # Consulta para los MÁXIMOS valores (Top 3) - Ordenado por Valor Unitario Descendente
        query_max = text(f'SELECT {cols_select} FROM importaciones WHERE {where_clause} ORDER BY ({sql_unit_val}) DESC LIMIT 3')
        df_max = pd.read_sql(query_max, engine, params=params)

        # Consulta para los MÍNIMOS valores (Top 3) - Ordenado por Valor Unitario Ascendente
        query_min = text(f'SELECT {cols_select} FROM importaciones WHERE {where_clause} ORDER BY ({sql_unit_val}) ASC LIMIT 3')
        df_min = pd.read_sql(query_min, engine, params=params)
        
        if df_max.empty or df_min.empty:
            return {'error': 'No se encontraron registros con los filtros proporcionados.'}

        # Convertimos los resultados a listas de diccionarios
        rec_max_list = df_max.to_dict('records')
        rec_min_list = df_min.to_dict('records')

        # Función auxiliar para procesar cada registro
        def procesar_registro(row):
            fob = row.get('FOB DOLAR', 0)
            cant = row.get('CANTIDAD', 0)
            if cant and cant > 0:
                unitario = fob / cant
            else:
                unitario = 0
            row['VALOR_UNITARIO_FMT'] = f"${unitario:,.2f}"
            row['FOB_FMT'] = f"${fob:,.2f}"
            row['CANTIDAD_FMT'] = f"{cant:,.0f}"
            
            # Formatear nuevas columnas
            for col in ['VALOR LISTA', 'VALOR PLANILLA', 'VALOR RES']:
                val = row.get(col)
                val = val if pd.notna(val) else 0
                row[f"{col.replace(' ', '_')}_FMT"] = f"${val:,.2f}"
            
            # Asegurar que observación no sea NaN
            row['OBSERVACION'] = row.get('OBSERVACION') if pd.notna(row.get('OBSERVACION')) else ''
            
            if pd.notna(row['OFICIALIZACION']):
                row['OFICIALIZACION'] = pd.to_datetime(row['OFICIALIZACION']).strftime('%d/%m/%Y')
            return row

        return {
            'max_records': [procesar_registro(r) for r in rec_max_list],
            'min_records': [procesar_registro(r) for r in rec_min_list]
        }

    except Exception as e:
        return {'error': f'Ocurrió un error procesando el archivo: {str(e)}'}

if __name__ == '__main__':
    app.run(debug=True)
