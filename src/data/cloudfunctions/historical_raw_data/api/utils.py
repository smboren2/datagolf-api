from google.cloud import bigquery

def upload_to_bq(table_id, dataframe, schema, write_type="WRITE_TRUNCATE"):
    """BigQuery Table Truncate/Append

    Uploads data to Google BigQuery. 

    Args:
        table_id: a table ID in the format <projectid.dataset.table>
        dataframe: a pandas dataframe containing the data to be uploaded
        schema: provide a schema with a list containing bigquery.Schema() calls
        write_type: 
            - "WRITE_TRUNCATE" will remove all data from the table and upload the given dataframe
            - "WRITE_APPEND" will append the given dataframe to the existing table

    Returns:
        Printed text indicating if the loaded data was successful
    """

    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition=write_type, 
        autodetect=False, 
        source_format=bigquery.SourceFormat.CSV
    )
    
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
    
    table = client.get_table(table_id)
    
    if write_type=="WRITE_TRUNCATE":
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    elif write_type=="WRITE_APPEND":
        print(f"Loaded {dataframe.shape[0]} rows and {dataframe.shape[1]} columns to {table_id}")
    else:
        print(f"No data loaded to {table_id}")