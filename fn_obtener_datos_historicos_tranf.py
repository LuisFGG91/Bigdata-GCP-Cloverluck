import requests
import json
from google.cloud import storage, bigquery
from datetime import datetime
import os

def create_table_if_not_exists(client, dataset_id, table_id, schema):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table {table_id} created.")

def upload_data_to_bigquery(client, dataset_id, table_id, rows_to_insert):
    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors != []:
        print(f"Errors occurred while inserting rows into table {table_id}: {errors}")

def process_historical_data(request):
    try:
        # Obtener la fecha actual
        now = datetime.now()
        fecha = now.strftime('%Y-%m-%d')
        periodo_de_carga = fecha  # Asumimos que el periodo de carga es la fecha actual
        
        # Inicializar el cliente de BigQuery
        client = bigquery.Client()

        # Especificar el ID del dataset
        dataset_id = 'transporte_publico'

        # Definir los esquemas para las tablas
        schemas = {
            'agency': [
                bigquery.SchemaField('agency_id', 'STRING'),
                bigquery.SchemaField('agency_name', 'STRING'),
                bigquery.SchemaField('agency_url', 'STRING'),
                bigquery.SchemaField('agency_timezone', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'calendar': [
                bigquery.SchemaField('service_id', 'STRING'),
                bigquery.SchemaField('monday', 'INTEGER'),
                bigquery.SchemaField('tuesday', 'INTEGER'),
                bigquery.SchemaField('wednesday', 'INTEGER'),
                bigquery.SchemaField('thursday', 'INTEGER'),
                bigquery.SchemaField('friday', 'INTEGER'),
                bigquery.SchemaField('saturday', 'INTEGER'),
                bigquery.SchemaField('sunday', 'INTEGER'),
                bigquery.SchemaField('start_date', 'STRING'),
                bigquery.SchemaField('end_date', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'calendar_dates': [
                bigquery.SchemaField('service_id', 'STRING'),
                bigquery.SchemaField('date', 'STRING'),
                bigquery.SchemaField('exception_type', 'INTEGER'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'feed_info': [
                bigquery.SchemaField('feed_publisher_name', 'STRING'),
                bigquery.SchemaField('feed_publisher_url', 'STRING'),
                bigquery.SchemaField('feed_lang', 'STRING'),
                bigquery.SchemaField('feed_start_date', 'STRING'),
                bigquery.SchemaField('feed_end_date', 'STRING'),
                bigquery.SchemaField('feed_version', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'frequencies': [
                bigquery.SchemaField('trip_id', 'STRING'),
                bigquery.SchemaField('start_time', 'STRING'),
                bigquery.SchemaField('end_time', 'STRING'),
                bigquery.SchemaField('headway_secs', 'INTEGER'),
                bigquery.SchemaField('exact_times', 'INTEGER'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'routes': [
                bigquery.SchemaField('route_id', 'STRING'),
                bigquery.SchemaField('agency_id', 'STRING'),
                bigquery.SchemaField('route_short_name', 'STRING'),
                bigquery.SchemaField('route_long_name', 'STRING'),
                bigquery.SchemaField('route_desc', 'STRING'),
                bigquery.SchemaField('route_type', 'STRING'),
                bigquery.SchemaField('route_url', 'STRING'),
                bigquery.SchemaField('route_color', 'STRING'),
                bigquery.SchemaField('route_text_color', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'shapes': [
                bigquery.SchemaField('shape_id', 'STRING'),
                bigquery.SchemaField('shape_pt_lat', 'FLOAT'),
                bigquery.SchemaField('shape_pt_lon', 'FLOAT'),
                bigquery.SchemaField('shape_pt_sequence', 'INTEGER'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'stop_times': [
                bigquery.SchemaField('trip_id', 'STRING'),
                bigquery.SchemaField('arrival_time', 'STRING'),
                bigquery.SchemaField('departure_time', 'STRING'),
                bigquery.SchemaField('stop_id', 'STRING'),
                bigquery.SchemaField('stop_sequence', 'INTEGER'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'stops': [
                bigquery.SchemaField('stop_id', 'STRING'),
                bigquery.SchemaField('stop_code', 'STRING'),
                bigquery.SchemaField('stop_name', 'STRING'),
                bigquery.SchemaField('stop_lat', 'FLOAT'),
                bigquery.SchemaField('stop_lon', 'FLOAT'),
                bigquery.SchemaField('stop_url', 'STRING'),
                bigquery.SchemaField('wheelchair_boarding', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ],
            'trips': [
                bigquery.SchemaField('route_id', 'STRING'),
                bigquery.SchemaField('service_id', 'STRING'),
                bigquery.SchemaField('trip_id', 'STRING'),
                bigquery.SchemaField('trip_headsign', 'STRING'),
                bigquery.SchemaField('direction_id', 'INTEGER'),
                bigquery.SchemaField('shape_id', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('periodo_de_carga', 'STRING')
            ]
        }

        # Crear tablas si no existen
        for table_id, schema in schemas.items():
            create_table_if_not_exists(client, dataset_id, table_id, schema)

        # Inicializar el cliente de Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket('transporte-publico-red')
        prefix = f'datos_historicos/{fecha}/'

        # Función para limpiar el BOM de los nombres de campo
        def clean_bom(field):
            return field.replace('\ufeff', '')

        # Procesar y subir cada archivo
        for table_id in schemas.keys():
            blob = bucket.blob(f'{prefix}{table_id}.txt')
            content = blob.download_as_string().decode('utf-8')
            lines = content.splitlines()
            headers = [clean_bom(header) for header in lines[0].split(',')]
            rows = [dict(zip(headers, line.split(','))) for line in lines[1:]]

            # Añadir los campos created_at y periodo_de_carga a cada fila
            for row in rows:
                row['created_at'] = datetime.now().isoformat()
                row['periodo_de_carga'] = periodo_de_carga

            # Insertar datos en BigQuery
            for i in range(0, len(rows), 500):  # Dividir en lotes de 500 filas
                upload_data_to_bigquery(client, dataset_id, table_id, rows[i:i+500])

        return 'Datos históricos procesados y almacenados en BigQuery'
    except Exception as e:
        print(f'Error al procesar los datos históricos: {e}')
        return f'Error al procesar los datos históricos: {e}'
