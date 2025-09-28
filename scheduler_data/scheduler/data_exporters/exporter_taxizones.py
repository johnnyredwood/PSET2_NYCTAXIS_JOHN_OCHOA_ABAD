from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

SNOWFLAKE_ACCOUNT = get_secret_value('ACCOUNT_ID')
SNOWFLAKE_USER = get_secret_value('SNOWFLAKE_USER_TRANSFORMER')
SNOWFLAKE_PRIVATE_KEY = get_secret_value('RSA_PRIVATE')
SNOWFLAKE_DATABASE = get_secret_value('DATABASE')
SNOWFLAKE_SCHEMA = get_secret_value('SCHEMA')
SNOWFLAKE_WAREHOUSE = get_secret_value('WAREHOUSE')
SNOWFLAKE_TABLE = 'NEWYORK_TAXIS_ZONES3'

def get_snowflake_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        private_key=SNOWFLAKE_PRIVATE_KEY,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    ) #retorno de la conexión con Snowflake

@data_exporter
def export_data_to_snowflake(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#snowflake
    """

    try:
        conn = get_snowflake_conn() 
        write_pandas(conn=conn,df=df,table_name=SNOWFLAKE_TABLE,auto_create_table=True)
        print("Data enviada correctamente a Snowflake para Zonas de Taxis de NY")

    except Exception as e:
        print(f"Error al cargar el archivo a Snowflake: {e}")
        raise
