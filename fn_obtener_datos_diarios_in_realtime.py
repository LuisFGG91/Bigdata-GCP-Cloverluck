# Importar paquetes necesarios
# fn_obtener_datos_diarios_in_realtime
import requests
import json
from google.cloud import pubsub_v1
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

def publish_to_pubsub(data):
    project_id = "eva-2-duocuc-clk"  # Reemplaza con tu ID de proyecto
    topic_id = "get_daily_data"  # Reemplaza con tu ID de topic

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        message = json.dumps(data).encode("utf-8")
        future = publisher.publish(topic_path, message)
        print(f'Published message to {topic_path}: {future.result()}')
    except Exception as e:
        print(f'Error publishing message to Pub/Sub: {e}')

def get_daily_data(request):
    try:
        # Obtener la fecha actual
        now = datetime.now()
        fecha = now.strftime('%Y-%m-%d')

        servicios = get_servicios_diarios()
        if not servicios:
            return 'Error al obtener los servicios diarios'

        for codsint in servicios:  # Recorrer directamente la lista de códigos
            detalles = get_detalles_recorrido(codsint)
            if detalles:
                # Publicar detalles al Pub/Sub
                publish_to_pubsub(detalles)
            else:
                print(f'No se pudieron obtener los detalles para el recorrido {codsint}')

        return 'Datos diarios obtenidos y publicados en Pub/Sub'
    except Exception as e:
        print(f'Error en el procesamiento de datos diarios: {e}')
        return 'Error en el procesamiento de datos diarios'
