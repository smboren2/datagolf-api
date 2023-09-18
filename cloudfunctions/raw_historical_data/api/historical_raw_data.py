import api.get_dg_api_data as main

def get_round_stroke_events(**kwargs):
    return main.get_dg_api_data('historical-raw-data', 'event-list', **kwargs)

def get_round_stroke_data(**kwargs):
    return main.get_dg_api_data('historical-raw-data', 'rounds', **kwargs)