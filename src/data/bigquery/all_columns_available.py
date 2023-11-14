import numpy as np
from google.cloud import bigquery

def all_columns_available(table_id, dataframe):
    """Compares records found in BigQuery table with fetched dataframe of new data

    Args:
        table_id: a table ID in the format <projectid.dataset.table>
        dataframe: a pandas dataframe containing the data fetched from API

    Returns:
        True or False
    """
    client = bigquery.Client()

    table = client.get_table(table_id)
    table_cols = ["{0}".format(schema.name) for schema in table.schema]
    
    missing_cols = np.array(table_cols)[~np.array([item in dataframe.columns for item in table_cols])].tolist()
    
    return all([item in dataframe.columns for item in table_cols])