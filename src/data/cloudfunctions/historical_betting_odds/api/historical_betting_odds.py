import api.get_dg_api_data as main
import time

def get_historical_odds_events(**kwargs):
    return main.get_dg_api_data('historical-odds', 'event-list', **kwargs)


def get_historical_odds_outrights(**kwargs):
    time.sleep(1)
    return main.get_dg_api_data('historical-odds', 'outrights', **kwargs)


def get_historical_odds_matchups(**kwargs):
    time.sleep(1)
    return main.get_dg_api_data('historical-odds', 'matchups', **kwargs)