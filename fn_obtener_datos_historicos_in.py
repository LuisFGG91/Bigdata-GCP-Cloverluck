# Importar paquetes necesarios
import requests
import zipfile
import os
from google.cloud import storage
from datetime import datetime

def download_and_extract_zip(request):
    # Obtener la fecha actual
    now = datetime.now()
    fecha = now.strftime('%Y-%m-%d')

    url = 'https://www.dtpm.cl/descargas/gtfs/GTFS-V124-PO20240601.zip'
    response = requests.get(url)
    zip_path = '/tmp/data.zip'
    
    # Guardar el archivo ZIP
    with open(zip_path, 'wb') as file:
        file.write(response.content)
    
    # Extraer el archivo ZIP
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('/tmp')

    # Subir los archivos extraídos a Cloud Storage
    client = storage.Client()
    bucket = client.bucket('transporte-publico-red')
    for root, dirs, files in os.walk('/tmp'):
        for file in files:
            if file.endswith(".txt"):  # Ajustar según los tipos de archivos esperados
                blob = bucket.blob(f'datos_historicos/{fecha}/{file}')
                blob.upload_from_filename(os.path.join(root, file))
    return 'Datos históricos descargados y almacenados en Cloud Storage'
