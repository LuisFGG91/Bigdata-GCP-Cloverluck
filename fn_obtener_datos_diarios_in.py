# Importar paquetes necesarios
import requests
import json
from google.cloud import storage
from datetime import datetime

def get_servicios_diarios():
    try:
        url = 'https://www.red.cl/restservice_v2/rest/getservicios/all'
        response = requests.get(url)
        response.raise_for_status()  # Esto lanzará una excepción para códigos de estado 4xx/5xx
        return response.json()  # Devuelve una lista de códigos de servicios
    except requests.exceptions.RequestException as e:
        print(f'Error al obtener servicios diarios: {e}')
        return None

def get_detalles_recorrido(codsint):
    try:
        url = f'https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={codsint}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error al obtener detalles del recorrido {codsint}: {e}')
        return None

def get_daily_data(request):
    try:
        # Obtener la fecha actual
        now = datetime.now()
        fecha = now.strftime('%Y-%m-%d')

        servicios = get_servicios_diarios()
        if not servicios:
            return 'Error al obtener los servicios diarios'

        client = storage.Client()
        bucket = client.bucket('transporte-publico-red')

        for codsint in servicios:  # Recorrer directamente la lista de códigos
            detalles = get_detalles_recorrido(codsint)
            if detalles:
                # Crear un archivo separado para cada recorrido
                blob = bucket.blob(f'datos_diarios/{fecha}/{codsint}.json')
                blob.upload_from_string(json.dumps(detalles), content_type='application/json')
            else:
                print(f'No se pudieron obtener los detalles para el recorrido {codsint}')

        return 'Datos diarios obtenidos y almacenados en Cloud Storage'
    except Exception as e:
        print(f'Error en el procesamiento de datos diarios: {e}')
        return 'Error en el procesamiento de datos diarios'
