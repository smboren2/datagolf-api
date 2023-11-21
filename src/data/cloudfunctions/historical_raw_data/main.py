import pandas as pd
from os import getenv
from google.cloud import bigquery

import api.historical_raw_data as hrd
import api.utils as utils

def update_historical_raw_data(request):

    dg_api = getenv("DG_API")
    projectid = getenv('GCP_PROJECT_ID')
    tablename = getenv('BQ_TABLE')
    datasetnm = 'event-list'

    bq_table = projectid + '.' + tablename + '.' + datasetnm

    client = bigquery.Client()

    primary_params = {'file_format': 'csv', 'key': dg_api}

    # query current table for events
    query_job = client.query(f"SELECT CONCAT(calendar_year, '-', tour, '-', event_id) AS event_concat FROM `{bq_table}`")

    bq_events = query_job.result().to_dataframe()['event_concat']

    # get event data from datagolf API
    dg_df = hrd.get_round_stroke_events(**primary_params)
    
    id_cols = ['calendar_year','tour','event_id'] # these must match query above
    
    dg_events = dg_df[id_cols].drop_duplicates().apply(lambda x: '-'.join(x.astype(str)), axis=1)

    if all(dg_events.isin(bq_events)):
        print("Data is up-to-date.")
        return "OK"
    else:
        print("Let's get the events we're missing...\n")

        missing_events = dg_events[~dg_events.isin(bq_events)]
        print(f"There are a total of {len(missing_events)} missing events.\n")
        missing_events_df = pd.DataFrame([x.split("-") for x in missing_events], columns = ['year','tour','event_id'])

        api_params = missing_events_df.to_dict(orient="records")

        event_list = list(map(lambda x: hrd.get_round_stroke_data(**{**primary_params, **x}), api_params))
        event_df = pd.concat(event_list)

        # get table schema
        table = client.get_table(f'{projectid}.{tablename}.rounds')
        table_cols = [f"{schema.name}" for schema in table.schema]

        if set(table_cols).issubset(event_df.columns):
            print("All columns are available and ready for appending\n")
            df_to_append = event_df[table_cols]
        else:
            print("Data returned from API is missing columns")
            return "OK"

        rounds_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        
        utils.upload_to_bq(
            table_id = f'{projectid}.{tablename}.rounds', 
            dataframe = df_to_append, 
            schema = rounds_schema,
            write_type = "WRITE_APPEND"
        )
        
        # update events
        table = client.get_table(f'{projectid}.{tablename}.event-list')
        events_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]

        events_to_append = dg_df[~dg_events.isin(bq_events)]
        
        utils.upload_to_bq(
            table_id = f'{projectid}.{tablename}.event-list', 
            dataframe = events_to_append, 
            schema = events_schema, 
            write_type="WRITE_APPEND"
        )
        
        df_to_print = dg_df[dg_events.isin(missing_events)][['tour','event_name']].sort_values(['tour', 'event_name']).to_dict(orient='records')
        
        print('\nThe following events have been uploaded to BigQuery:\n')
        for i in df_to_print:
            print('\t[' + i['tour'].upper() + ']' + '\t' + i['event_name'])
    
    return "OK"
