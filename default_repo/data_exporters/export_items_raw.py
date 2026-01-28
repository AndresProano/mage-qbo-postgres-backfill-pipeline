from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime
import psycopg2
import json
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def log(phase, message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{phase}] {message}")

@data_exporter
def export_data(data, *args, **kwargs):

    if data is None or data.empty:
        log("WARN", "No llegaron datos para exportar. Pipeline finalizado.")
        return

    try:
        db_host = get_secret_value('POSTGREST_HOST')
        db_name = get_secret_value('POSTGRES_DB')
        db_user = get_secret_value('POSTGRES_USER')
        db_pass = get_secret_value('POSTGRES_PASSWORD')
    except Exception as e:
        log("CONFIG_ERROR", f"Error en credenciales, {e}")
        return

    log("DATA_PROCESS", f"Guardando {len(data)} datos")

    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO raw.qb_items_backfill (
        id, payload, ingested_at_utc, extract_window_start_utc, 
        extract_window_end_utc, page_number, page_size, request_payload
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        payload = EXCLUDED.payload,
        ingested_at_utc = EXCLUDED.ingested_at_utc,
        extract_window_start_utc = EXCLUDED.extract_window_start_utc,
        extract_window_end_utc = EXCLUDED.extract_window_end_utc,
        page_number = EXCLUDED.page_number,
        page_size = EXCLUDED.page_size,
        request_payload = EXCLUDED.request_payload;
    """

    success_count = 0
    error_count = 0

    try:
        for _, row in data.iterrows():

            try:
                cursor.execute(insert_query, (
                    str(row['id']),
                    json.dumps(row['payload']),
                    row['ingested_at_utc'],
                    row['extract_window_start_utc'],
                    row['extract_window_end_utc'],
                    row['page_number'],
                    row['page_size'],
                    json.dumps(row['request_payload'])
                ))
                success_count += 1
            except Exception as e:
                error_count += 1
                log("ROW_ERROR", f"Fallo en ID {row.get('id', 'unknown')}: {e}")
                conn.rollback()
                raise e

        
        conn.commit()
        log("METRIC", "Datos guardados correctamente")
    except Exception as e:
        conn.rollback()
        log("CRITICAL", f"Error al guardar datos: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()