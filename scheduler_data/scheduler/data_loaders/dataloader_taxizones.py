import pandas as pd
import requests
from io import StringIO
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

@data_loader
def load_taxi_zone_data(output_green):
    """
    Data loader para crear DataFrame con datos del taxi_zone_lookup.csv
    """
    print(f"Descargando archivo CSV de: {TAXI_ZONE_URL}")
    
    try:
        # Descargar el archivo CSV
        resp = requests.get(TAXI_ZONE_URL, timeout=60)
        resp.raise_for_status()
        
        # Leer el CSV en un DataFrame
        df = pd.read_csv(StringIO(resp.text))
        
        print(f"Archivo de zonas de taxis bajado exitosamente")
        print(f"Columnas: {list(df.columns)}")

        return df
            
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        raise