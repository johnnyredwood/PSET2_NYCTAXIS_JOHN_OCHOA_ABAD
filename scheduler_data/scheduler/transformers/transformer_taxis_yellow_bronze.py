if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import subprocess
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

@transformer
def execute_dbt(*args, **kwargs):
    
    env_vars = {
        'SNOWFLAKE_ACCOUNT': get_secret_value('ACCOUNT_ID'),
        'SNOWFLAKE_USER': get_secret_value('SNOWFLAKE_USER_TRANSFORMER'),
        'SNOWFLAKE_PRIVATE_KEY': get_secret_value('RSA_PRIVATE'),
        'SNOWFLAKE_WAREHOUSE': get_secret_value('WAREHOUSE'),
        'SNOWFLAKE_DATABASE': get_secret_value('DATABASE'),
        'SNOWFLAKE_SCHEMA': get_secret_value('SCHEMA'),
        'SNOWFLAKE_ROLE_LOW_PRIVILEDGES': get_secret_value('ROLE_LOW_PRIVILEDGES'),
        'DBT_PROFILES_DIR': '/home/src/scheduler/dbt'
    }
    
    # Comandos dbt
    commands = [
        'dbt deps --profiles-dir /home/src/scheduler/dbt',
        'dbt run --select tag:bronze --profiles-dir /home/src/scheduler/dbt'
    ]
    
    results = []
    for cmd in commands:
        try:
            result = subprocess.run(
                cmd.split(),
                cwd='/home/src/scheduler/dbt/NY_TAXI_DBT',
                env={**os.environ.copy(), **env_vars},
                capture_output=True,
                text=True
            )
            results.append({
                'command': cmd,
                'success': result.returncode == 0,
                'output': result.stdout
            })
        except Exception as e:
            results.append({
                'command': cmd,
                'success': False,
                'error': str(e)
            })
    
    return results