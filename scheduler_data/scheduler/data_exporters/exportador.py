import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_snowflake(*args, **kwargs):
    """
    Exporta todos los batches del generador a Snowflake.
    """
    # Debug: ver todas las keys disponibles en kwargs
    print("🔍 Keys disponibles en kwargs:", list(kwargs.keys()))
    
    # Buscar el generador en diferentes keys posibles
    data_generator = None
    possible_keys = ['output_0', 'df', 'output', 'data']
    
    for key in possible_keys:
        if key in kwargs and kwargs[key] is not None:
            data_generator = kwargs[key]
            print(f"✅ Generador encontrado en key: {key}")
            break
    
    if data_generator is None:
        # Mostrar todas las keys y valores para debug
        print("📋 Todos los kwargs:")
        for key, value in kwargs.items():
            print(f"   {key}: {type(value)}")
        raise ValueError("No se encontró el generador en ninguna key conocida")
    
    # Configuración de Snowflake
    config = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    }
    
    # Verificar configuración
    for key in ['user', 'password', 'account', 'database']:
        if not config[key]:
            raise ValueError(f"Variable {key} no configurada en environment variables")
    
    # Conectar a Snowflake
    print("🔗 Conectando a Snowflake...")
    connection = connect(**config)
    
    total_batches = 0
    total_rows = 0
    
    try:
        print("🚀 Iniciando exportación a Snowflake...")
        
        # Procesar cada batch del generador
        for batch_df in data_generator:
            total_batches += 1
            
            # Exportar el batch actual
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=batch_df,
                table_name='yellow_taxi_trips',
                auto_create_table=True,
                overwrite=False
            )
            
            total_rows += nrows
            print(f"✅ Batch {total_batches}: {nrows} filas exportadas")
        
        print(f"🎉 Exportación completada: {total_batches} batches, {total_rows} filas")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        connection.close()
        print("🔒 Conexión cerrada")