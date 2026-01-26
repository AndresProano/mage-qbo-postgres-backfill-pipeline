import io
import pandas as pd
import requests
import base64
import time
from datetime import timedelta, datetime
import pytz
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def log(phase, message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{phase}] {message}")

def get_access_token():
    log("AUTH", "Solicitando nuevo token...")
    client_id = get_secret_value('QBO_CLIENT_ID')
    client_secret = get_secret_value('QBO_CLIENT_SECRET')
    refresh_token = get_secret_value('QBO_REFRESH_TOKEN')

    auth_str = f'{client_id}:{client_secret}'
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
    headers = {
        'Authorization' : f'Basic {b64_auth}',
        'Content-Type' : 'application/x-www-form-urlencoded',
        'Accept' : 'application/json'
    }

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code != 200:
            log("AUTH_ERROR", f"Fallo al renovar: {response.text}")
            raise Exception(f"Error while renewing token: {response.text}")
        return response.json().get('access_token')
    except Exception as e:
        log("AUTH_ERROR", str(e))
        raise e
    

@data_loader
def load_data(*args, **kwargs):

    now = datetime.now(pytz.utc)
    default_start = (now - timedelta(days=2)).strftime('%Y-%m-%d')
    default_end = now.strftime('%Y-%m-%d')

    start_date_str = kwargs.get('fecha_inicio', default_start)
    end_date_str = kwargs.get('fecha_fin', default_end)

    log("INIT", f"Iniciando extracción desde {start_date_str} hasta {end_date_str}")

    access_token = get_access_token()
    realm_id = get_secret_value('QBO_REALM_ID')

    #query = "SELECT * FROM Customer STARTPOSITION 1 MAXRESULTS 1"
    base_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{realm_id}/query?minorversion=65"

    headers = {
        'Authorization' : f'Bearer {access_token}',
        'Accept' : 'application/json',
        'Content-Type' : 'application/json'
    }

    all_records = []

    start_dt = pd.to_datetime(start_date_str).replace(tzinfo=pytz.UTC)
    end_dt = pd.to_datetime(end_date_str).replace(tzinfo=pytz.UTC)

    print(f"Rango de datos: {start_dt} hasta {end_dt}")

    current_date = start_dt

    while current_date <= end_dt:

        chunk_start_time = time.time()
        daily_pages = 0
        daily_rows = 0

        next_date = current_date + timedelta(days=1)

        start_fmt = current_date.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        end_fmt = next_date.strftime('%Y-%m-%dT%H:%M:%S-00:00')

        start_position = 1
        max_results = 1000

        while True:
            query = (
                f"SELECT * FROM Customer "
                f"WHERE MetaData.LastUpdatedTime >= '{start_fmt}' "
                f"AND MetaData.LastUpdatedTime <= '{end_fmt}' "
                f"STARTPOSITION {start_position} MAXRESULTS {max_results}"
            )

            max_retries = 5
            success = False

            for attempt in range(max_retries):
                try:
                    response = requests.get(base_url, headers=headers, params={'query':query})
                    if response.status_code == 401:
                        log("AUTH", 'Token expirado, obteniendo uno nuevo...')
                        access_token = get_access_token()
                        headers['Authorization'] = f"Bearer {access_token}"
                        continue

                    if response.status_code == 429:
                        wait_time = 2 ** attempt .
                        log("WARN", f"Rate Limit (429). Esperando {wait_time}s antes del reintento {attempt+1}/{max_retries}...")
                        time.sleep(wait_time)
                        continue

                    if response.status_code >= 500:
                        wait_time = 2 ** attempt
                        log("SERVER", f"Error Servidor ({response.status_code}). Esperando {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    if response.status_code != 200:
                        log("ERROR", f"Error al obtener datos del dia: {current_date}: {response.text}")
                        break
                    
                    success = True
                    break

                except Exception as e:
                    log("ERROR", "Error de conexion: {e}")
                    time.sleep(2 ** attempt)
                
                if not success:
                    log("ERROR", "Se extendieron los intentos para el dia: {current_date.date()}")
                    break

                data = response.json()
                batch = data.get('QueryResponse', {}).get('Customer', [])

                if not batch:
                    break

                daily_pages += 1
                daily_rows += len(batch)

                ingestion_time = datetime.now(pytz.utc)

                for item in batch:
                    record = {
                        'id': item['Id'], 
                        'payload': item,  
                        'ingested_at_utc': ingestion_time,
                        'extract_window_start_utc': start_fmt,
                        'extract_window_end_utc': end_fmt,
                        'page_number': (start_position // max_results) + 1,
                        'page_size': len(batch),
                        'request_payload': query 
                    }
                    all_records.append(record)

                start_position += max_results
                if len(batch) < max_results:
                    break
                

        chunk_duration = time.time() - chunk_start_time
    log("METRIC", f"Día: {current_date.date()} | Filas: {daily_rows} | Duración: {duration:.2f}s")

        if daily_rows > 0:
            log("REPORTE", f"Reporte tramo {current_data.date()}:")
            log("REPORTE", f"Paginas: {daily_pages}")
            log("REPORTE", f"Filas: {daily_rows}")
            log("REPORTE", f"Duracion: {chunk_duration:.2f}")
        else:
            log("VOLUMETRY_WARN", f"0 registros para {current_date.date()}")

        current_date = next_date
    
    log("DONE", f'Total: {len(all_records)} registros')
    return pd.DataFrame(all_records)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
