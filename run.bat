@echo off
REM Cambiar al directorio donde se encuentra este script para evitar errores de ruta
cd /d "%~dp0"

REM Activar el entorno virtual (asumiendo que la carpeta se llama 'venv')
call venv\Scripts\activate

REM Ejecutar la aplicación Flask
python app.py
pause