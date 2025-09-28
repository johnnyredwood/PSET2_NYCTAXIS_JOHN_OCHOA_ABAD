if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import os
from mage_ai.data_preparation.shared.secrets import get_secret_value


@custom
def export_snowflake_secrets(*args, **kwargs):

    secrets = [
        'SNOWFLAKE_USER_TRANSFORMER',
        'SNOWFLAKE_PASS',
        'ACCOUNT_ID',
        'WAREHOUSE',
        'DATABASE',
        'SCHEMA',
    ]

    for key in secrets:
        value = get_secret_value(key)
        if value:
            os.environ[key] = value

    print("Secrets exportados")
