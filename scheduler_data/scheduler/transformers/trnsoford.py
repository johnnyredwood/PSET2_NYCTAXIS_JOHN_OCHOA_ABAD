if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# In your transformation block
@transformer
def transform_data(df, *args, **kwargs):
    # df will be a single DataFrame from one of the yielded chunks
    # Process your data here
    return df
