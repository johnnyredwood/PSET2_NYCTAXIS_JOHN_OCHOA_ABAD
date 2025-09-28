import pandas as pd
import requests
from io import BytesIO
import pyarrow.parquet as pq
from datetime import datetime
import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import json
import os

CHECKPOINT_FILE = "checkpointTaxisGreen.json"

def save_checkpoint(year, month):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"year": year, "month": month}, f)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"year": 0, "month": 0}

# Datos para la conexión con Snowflake. Ver si se pueden poner en el io_config.yaml
SNOWFLAKE_ACCOUNT = get_secret_value('ACCOUNT_ID')
SNOWFLAKE_USER = get_secret_value('SNOWFLAKE_USER_TRANSFORMER')
SNOWFLAKE_PRIVATE_KEY = get_secret_value('RSA_PRIVATE')
SNOWFLAKE_DATABASE = get_secret_value('DATABASE')
SNOWFLAKE_SCHEMA = get_secret_value('SCHEMA')
SNOWFLAKE_WAREHOUSE = get_secret_value('WAREHOUSE')
SNOWFLAKE_TABLE = 'NY_TAXIS_GREEN_BRONZE' 

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/" #Tramo genérico de URL que contiene los datos
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/octet-stream"
}

# Cargar checkpoint
checkpoint = load_checkpoint()
REANUDACION_YEAR = checkpoint["year"]
REANUDACION_MONTH = checkpoint["month"]

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        private_key=SNOWFLAKE_PRIVATE_KEY,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    ) #retorno de la conexión con Snowflake

def create_table_with_constraints(conn):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
    VENDORID NUMBER,
    LPEP_PICKUP_DATETIME NUMBER,
    LPEP_DROPOFF_DATETIME NUMBER,
    PASSENGER_COUNT NUMBER,
    TRIP_DISTANCE FLOAT,
    RATECODEID NUMBER,
    STORE_AND_FWD_FLAG VARCHAR(1),
    PULOCATIONID NUMBER,
    DOLOCATIONID NUMBER,
    PAYMENT_TYPE NUMBER,
    FARE_AMOUNT FLOAT,
    EXTRA FLOAT,
    MTA_TAX FLOAT,
    TIP_AMOUNT FLOAT,
    TOLLS_AMOUNT FLOAT,
    EHAIL_FEE FLOAT,
    IMPROVEMENT_SURCHARGE FLOAT,
    TOTAL_AMOUNT FLOAT,
    TRIP_TYPE NUMBER,
    CONGESTION_SURCHARGE FLOAT,
    AIRPORT_FEE FLOAT,
    RUN_ID NUMBER,
    VENTANA_TEMPORAL NUMBER,
    LOTE_MES VARCHAR(50),
    MONTH NUMBER,
    YEAR NUMBER,
    PRIMARY KEY (LPEP_PICKUP_DATETIME, LPEP_DROPOFF_DATETIME, PULOCATIONID, DOLOCATIONID)
    )
    """
    cursor = conn.cursor() #Nos aseguramos idempotencia definiendo tabla con su llave primaria
    cursor.execute(create_table_sql)
    cursor.close()

@data_loader
def load_and_export_data(output_yellow):
    taxi_type = "green" #generamos segundo la tabla para taxis verdes
    ID = 1
    years = range(2015, 2026) #Carga de todos los datos de 2015-2025
    months = range(1, 13) #Carga de todos los datos de todos los meses
     
    conn = get_snowflake_conn() # Crear conexión con Snowflake
    
    try:

        create_table_with_constraints(conn)

        for year in years: #iteramos por cada año por cada mes
            if REANUDACION_YEAR != 0 and year < REANUDACION_YEAR:
                print(f"Saltando año {year} (ya procesado)")
                continue
            for month in months:
                if REANUDACION_YEAR == year and month <= REANUDACION_MONTH:
                    print(f"Saltando {year}-{month:02d} (ya procesado)")
                    continue
                if year==2025 and month>7:
                    break #Hasta aqui hay datos en la pagina de NEW YORK
                file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
                url = BASE_URL + file_name #URL para cada mes para cada año

                print(f"Descargando archivo parquet de: {url}")
                resp = requests.get(url, headers=HEADERS, timeout=60)
                resp.raise_for_status()

                parquet_file = pq.ParquetFile(BytesIO(resp.content)) #Lectura de datos de parquet
                lote = 1

                for batch in parquet_file.iter_batches(batch_size=1000000): #Iteración por batches de datos para procesamiento sin hacer kill a la RAM
                    df = batch.to_pandas()

                    # Agregar metadatos solicitados en Deber para esquema Bronce
                    df['run_id'] = ID
                    df['ventana_temporal'] = datetime.now()
                    df['lote_mes'] = f"{lote}/{month}"
                    df['month'] = month
                    df['year'] = year

                    # Eliminar columnas problemáticas directamente

                    if 'cbd_congestion_fee' in df.columns:
                        df = df.drop(columns=['cbd_congestion_fee']) #Eliminar columna que solo existe en pocos archivos

                    df.columns = [c.upper() for c in df.columns] #Homogeneizar nombres de columnas
                    
                    # Exportar directamente datos a tabla de Snowflake
                    from snowflake.connector.pandas_tools import write_pandas
                    write_pandas(
                        conn=conn,
                        df=df,
                        table_name=SNOWFLAKE_TABLE,
                        auto_create_table=True
                    )

                    print(f"{file_name}: lote {lote}, {len(df)} filas exportadas correctamente")
                    lote += 1

                # Guardar checkpoint después de cada mes
                save_checkpoint(year, month)
                print(f"Checkpoint guardado: {year}-{month:02d}")
                ID += 1

    except Exception as e:
        print(f"Error en carga de datos: {e}")
    finally:
        conn.close()

    return "Procesamiento completado. Todo lo de taxis se encuentra en Snowflake"