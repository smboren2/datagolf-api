import pandas as pd
from os import getenv
from google.cloud import bigquery

import api.historical_betting_odds as odds
import api.utils as utils

def update_historical_betting_odds(request): 

    # constants 
    dg_api = getenv("DG_API")
    projectid = getenv('GCP_PROJECT_ID')
    tablename = getenv('BQ_TABLE')
    datasetnm = 'event-list'

    # --- available sportsbooks + markets --- #
    books = [
        '5dimes', 'bet365', 'betcris', 'betmgm', 'betonline', 
        'bovada', 'circa', 'draftkings', 'fanduel', 'pinnacle', 
        'sportsbook', 'williamhill', 'unibet'
    ]

    markets = ['win', 'top_5', 'top_10', 'top_20', 'make_cut', 'mc']
    # --------------------------------------- #

    bq_table = projectid + '.' + tablename + '.' + datasetnm

    client = bigquery.Client()

    primary_params = {'file_format': 'csv', 'key': dg_api}

    # query current table for events
    query_job = client.query(f"SELECT CONCAT(calendar_year, '-', event_id) AS event_concat FROM `{bq_table}`")

    bq_events = query_job.result().to_dataframe()['event_concat']

    # get event data from datagolf API
    dg_df = odds.get_historical_odds_events(**primary_params)

    id_cols = ['calendar_year','event_id'] # these must match query above
    dg_events = dg_df[id_cols].drop_duplicates().apply(lambda x: '-'.join(x.astype(str)), axis=1)

    if all(dg_events.isin(bq_events)):
        print("Data is up-to-date.")
        return "OK"
    else:
        print("Let's get the events we're missing...\n")

        missing_events = dg_events[~dg_events.isin(bq_events)]
        print(f"There are a total of {len(missing_events)} missing events.\n")
        
        missing_events_df = pd.DataFrame([x.split("-") for x in missing_events], columns = ['year','event_id'])
        missing_events_df_full = dg_df.loc[~dg_events.isin(bq_events)].reset_index(drop=True)
        
        # building API params separate for matchups and outrights
        # --- MATCHUPS
        matchups = missing_events_df_full.loc[missing_events_df_full['matchups'] == 'yes'][['calendar_year','event_id']]

        matchups_params = matchups.copy()
        matchups_params.rename(columns={'calendar_year':'year'}, inplace=True)
        matchups_params = matchups_params[['event_id','year']]

        matchup_final = pd.concat([matchups_params]*len(books), ignore_index=True)
        matchup_final.sort_values(['event_id','year'], inplace=True)
        matchup_final['book'] = books*matchups.shape[0]
        matchup_final['odds_type'] = 'decimal'

        matchups_final_dict = matchup_final.to_dict(orient="records")

        # query DataGolf API with new events
        matchups_list = list(map(lambda x: odds.get_historical_odds_matchups(**{**primary_params, **x}), matchups_final_dict))
        remove_skips = [item for item in matchups_list if 'skip' not in item]
        matchups_df = pd.concat(remove_skips)
        
        # --- OUTRIGHTS
        outright = missing_events_df_full.loc[missing_events_df_full['outrights'] == 'yes']

        outright_params = outright.copy()
        outright_params.rename(columns={'calendar_year':'year'}, inplace=True)
        outright_params = outright_params[['event_id','year']]

        outright_final = pd.concat([outright_params]*len(books), ignore_index=True)
        outright_final.sort_values(['event_id','year'], inplace=True)
        outright_final['book'] = books*outright.shape[0]
        outright_final['odds_type'] = 'decimal'

        # explode DF to add outright types
        market_repeats = outright_final.shape[0]
        all_outrights = pd.concat([outright_final]*len(markets)).sort_values(['event_id','year','book'])
        all_outrights['market'] = markets*market_repeats

        all_outrights_dict = all_outrights.to_dict(orient="records")
        
        # query DataGolf API with new events
        outright_list = list(map(lambda x: odds.get_historical_odds_outrights(**{**primary_params, **x}), all_outrights_dict))
        remove_skips = [item for item in outright_list if 'skip' not in item]
        outright_df = pd.concat(remove_skips)
        
        
        # get matchup table schema
        matchups_table = client.get_table(f'{projectid}.{tablename}.matchups')
        matchups_table_cols = [f"{schema.name}" for schema in matchups_table.schema]
        matchups_ready = set(matchups_table_cols).issubset(matchups_df.columns)

        # get outright table schema
        outright_table = client.get_table(f'{projectid}.{tablename}.outrights')
        outright_table_cols = [f"{schema.name}" for schema in outright_table.schema]
        outright_ready = set(outright_table_cols).issubset(outright_df.columns)

        if (matchups_ready & outright_ready):
            print("All columns are available and ready for appending\n")

            matchups_df['close_time'] = pd.to_datetime(matchups_df['close_time'])
            matchups_df['open_time'] = pd.to_datetime(matchups_df['open_time'])
            matchups_df['p3_dg_id'] = matchups_df['p3_dg_id'].astype('Int64')
            matchups_df['event_completed'] = pd.to_datetime(matchups_df['event_completed']).dt.date
            matchups_to_append = matchups_df[matchups_table_cols]

            outright_df['close_time'] = pd.to_datetime(outright_df['close_time'])
            outright_df['open_time'] = pd.to_datetime(outright_df['open_time'])
            outright_df['event_completed'] = pd.to_datetime(outright_df['event_completed']).dt.date
            outright_to_append = outright_df[outright_table_cols]

            print(f'Retrieved {len(missing_events)} new ODDS events')
            print('New ODDS events ready for upload to BigQuery...\n')

            matchups_schema = [{'name':i.name, 'type':i.field_type} for i in matchups_table.schema]
            outright_schema = [{'name':i.name, 'type':i.field_type} for i in outright_table.schema]

            utils.upload_to_bq(
                    table_id = f'{projectid}.{tablename}.matchups', 
                    dataframe = matchups_to_append, 
                    schema = matchups_schema,
                    write_type = "WRITE_APPEND"
            )

            utils.upload_to_bq(
                    table_id = f'{projectid}.{tablename}.outrights', 
                    dataframe = outright_to_append, 
                    schema = outright_schema,
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

            df_to_print = dg_df[dg_events.isin(missing_events)][['event_id','event_name']].sort_values(['event_id', 'event_name']).to_dict(orient='records')

            print('\nThe following events have been uploaded to BigQuery:\n')
            for i in df_to_print:
                print('\t[' + str(i['event_id']) + ']' + '\t' + i['event_name'])
            return "OK"

        else:
            print("Data returned from API is missing columns")
            return "OK"
