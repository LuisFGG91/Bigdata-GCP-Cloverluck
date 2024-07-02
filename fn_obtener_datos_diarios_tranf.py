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
    if not rows_to_insert:
        print(f"No rows to insert for table {table_id}")
        return
    
    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors != []:
        print(f"Errors occurred while inserting rows into table {table_id}: {errors}")

def process_json_to_bigquery(request):
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
            'negocios': [
                bigquery.SchemaField('negocio_id', 'INTEGER'),
                bigquery.SchemaField('nombre', 'STRING'),
                bigquery.SchemaField('color', 'STRING'),
                bigquery.SchemaField('url', 'STRING')
            ],
            'horarios': [
                bigquery.SchemaField('recorrido_id', 'STRING'),
                bigquery.SchemaField('ida_o_regreso', 'STRING'),
                bigquery.SchemaField('tipoDia', 'STRING'),
                bigquery.SchemaField('inicio', 'STRING'),
                bigquery.SchemaField('fin', 'STRING')
            ],
            'paths': [
                bigquery.SchemaField('recorrido_id', 'STRING'),
                bigquery.SchemaField('ida_o_regreso', 'STRING'),
                bigquery.SchemaField('lat', 'FLOAT'),
                bigquery.SchemaField('lon', 'FLOAT')
            ],
            'paraderos': [
                bigquery.SchemaField('recorrido_id', 'STRING'),
                bigquery.SchemaField('ida_o_regreso', 'STRING'),
                bigquery.SchemaField('paradero_id', 'INTEGER'),
                bigquery.SchemaField('cod', 'STRING'),
                bigquery.SchemaField('num', 'INTEGER'),
                bigquery.SchemaField('lat', 'FLOAT'),
                bigquery.SchemaField('lon', 'FLOAT'),
                bigquery.SchemaField('name', 'STRING'),
                bigquery.SchemaField('comuna', 'STRING'),
                bigquery.SchemaField('type', 'INTEGER'),
                bigquery.SchemaField('servicios', 'STRING'),
                bigquery.SchemaField('stopId', 'INTEGER'),
                bigquery.SchemaField('stopCoordenadaX', 'FLOAT'),
                bigquery.SchemaField('stopCoordenadaY', 'FLOAT'),
                bigquery.SchemaField('eje', 'STRING'),
                bigquery.SchemaField('codSimt', 'STRING'),
                bigquery.SchemaField('distancia', 'FLOAT')
            ],
            'servicios': [
                bigquery.SchemaField('paradero_id', 'INTEGER'),
                bigquery.SchemaField('id', 'INTEGER'),
                bigquery.SchemaField('cod', 'STRING'),
                bigquery.SchemaField('destino', 'STRING'),
                bigquery.SchemaField('orden', 'INTEGER'),
                bigquery.SchemaField('color', 'STRING'),
                bigquery.SchemaField('negocio_nombre', 'STRING'),
                bigquery.SchemaField('negocio_color', 'STRING'),
                bigquery.SchemaField('recorrido_destino', 'STRING'),
                bigquery.SchemaField('itinerario', 'BOOLEAN'),
                bigquery.SchemaField('codigo', 'STRING')
            ]
        }

        # Crear tablas si no existen
        for table_id, schema in schemas.items():
            create_table_if_not_exists(client, dataset_id, table_id, schema)

        # Inicializar el cliente de Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket('transporte-publico-red')
        prefix = f'datos_diarios/{fecha}/'

        # Listar los blobs en la carpeta del día actual
        blobs = list(storage_client.list_blobs(bucket, prefix=prefix))

        # Procesar y subir cada archivo JSON
        for blob in blobs:
            recorrido_id = os.path.splitext(os.path.basename(blob.name))[0]
            content = blob.download_as_text()
            json_data = json.loads(content)

            negocio = json_data.get('negocio', {})
            ida = json_data.get('ida', {})
            regreso = json_data.get('regreso', {})

            # Negocios
            negocio_row = {
                'negocio_id': negocio.get('id'),                    
                'nombre': negocio.get('nombre'),
                'color': negocio.get('color'),
                'url': negocio.get('url')
            }
            upload_data_to_bigquery(client, dataset_id, 'negocios', [negocio_row])

            # Horarios de ida
            horarios_rows = []
            for horario in ida.get('horarios', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'ida',
                    'tipoDia': horario.get('tipoDia'),
                    'inicio': horario.get('inicio'),
                    'fin': horario.get('fin')
                }
                horarios_rows.append(row)
            upload_data_to_bigquery(client, dataset_id, 'horarios', horarios_rows)

            # Horarios de regreso
            horarios_rows = []
            for horario in regreso.get('horarios', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'regreso',
                    'tipoDia': horario.get('tipoDia'),
                    'inicio': horario.get('inicio'),
                    'fin': horario.get('fin')
                }
                horarios_rows.append(row)
            upload_data_to_bigquery(client, dataset_id, 'horarios', horarios_rows)

            # Paths de ida
            paths_rows = []
            for point in ida.get('path', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'ida',
                    'lat': point[0],
                    'lon': point[1]
                }
                paths_rows.append(row)
            upload_data_to_bigquery(client, dataset_id, 'paths', paths_rows)

            # Paths de regreso
            paths_rows = []
            for point in regreso.get('path', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'regreso',
                    'lat': point[0],
                    'lon': point[1]
                }
                paths_rows.append(row)
            upload_data_to_bigquery(client, dataset_id, 'paths', paths_rows)

            # Paraderos de ida
            paraderos_rows = []
            servicios_rows = []
            for paradero in ida.get('paraderos', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'ida',
                    'paradero_id': paradero.get('id'),
                    'cod': paradero.get('cod'),
                    'num': paradero.get('num'),
                    'lat': paradero.get('pos')[0],
                    'lon': paradero.get('pos')[1],
                    'name': paradero.get('name'),
                    'comuna': paradero.get('comuna'),
                    'type': paradero.get('type'),
                    'servicios': json.dumps(paradero.get('servicios')),
                    'stopId': paradero['stop']['stopId'],
                    'stopCoordenadaX': float(paradero['stop']['stopCoordenadaX']),
                    'stopCoordenadaY': float(paradero['stop']['stopCoordenadaY']),
                    'eje': paradero.get('eje'),
                    'codSimt': paradero.get('codSimt'),
                    'distancia': paradero.get('distancia')
                }
                paraderos_rows.append(row)

                # Procesar servicios
                for servicio in paradero.get('servicios', []):
                    servicio_row = {
                        'paradero_id': paradero.get('id'),
                        'id': servicio.get('id'),
                        'cod': servicio.get('cod'),
                        'destino': servicio.get('destino'),
                        'orden': servicio.get('orden'),
                        'color': servicio.get('color'),
                        'negocio_nombre': servicio['negocio'].get('nombre'),
                        'negocio_color': servicio['negocio'].get('color'),
                        'recorrido_destino': servicio['recorrido'].get('destino'),
                        'itinerario': servicio.get('itinerario'),
                        'codigo': servicio.get('codigo')
                    }
                    servicios_rows.append(servicio_row)
            upload_data_to_bigquery(client, dataset_id, 'paraderos', paraderos_rows)
            upload_data_to_bigquery(client, dataset_id, 'servicios', servicios_rows)

            # Paraderos de regreso
            paraderos_rows = []
            servicios_rows = []
            for paradero in regreso.get('paraderos', []):
                row = {
                    'recorrido_id': recorrido_id,
                    'ida_o_regreso': 'regreso',
                    'paradero_id': paradero.get('id'),
                    'cod': paradero.get('cod'),
                    'num': paradero.get('num'),
                    'lat': paradero.get('pos')[0],
                    'lon': paradero.get('pos')[1],
                    'name': paradero.get('name'),
                    'comuna': paradero.get('comuna'),
                    'type': paradero.get('type'),
                    'servicios': json.dumps(paradero.get('servicios')),
                    'stopId': paradero['stop']['stopId'],
                    'stopCoordenadaX': float(paradero['stop']['stopCoordenadaX']),
                    'stopCoordenadaY': float(paradero['stop']['stopCoordenadaY']),
                    'eje': paradero.get('eje'),
                    'codSimt': paradero.get('codSimt'),
                    'distancia': paradero.get('distancia')
                }
                paraderos_rows.append(row)

                # Procesar servicios
                for servicio in paradero.get('servicios', []):
                    servicio_row = {
                        'paradero_id': paradero.get('id'),
                        'id': servicio.get('id'),
                        'cod': servicio.get('cod'),
                        'destino': servicio.get('destino'),
                        'orden': servicio.get('orden'),
                        'color': servicio.get('color'),
                        'negocio_nombre': servicio['negocio'].get('nombre'),
                        'negocio_color': servicio['negocio'].get('color'),
                        'recorrido_destino': servicio['recorrido'].get('destino'),
                        'itinerario': servicio.get('itinerario'),
                        'codigo': servicio.get('codigo')
                    }
                    servicios_rows.append(servicio_row)
            upload_data_to_bigquery(client, dataset_id, 'paraderos', paraderos_rows)
            upload_data_to_bigquery(client, dataset_id, 'servicios', servicios_rows)

        return 'Datos procesados y almacenados en BigQuery'
    except Exception as e:
        print(f'Error al procesar los datos: {e}')
        return f'Error al procesar los datos: {e}'
