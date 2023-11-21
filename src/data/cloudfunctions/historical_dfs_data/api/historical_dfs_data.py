import api.get_dg_api_data as main

def get_dfs_events(**kwargs):
    return main.get_dg_api_data('historical-dfs-data', 'event-list', **kwargs)

def get_dfs_data(**kwargs):
    return main.get_dg_api_data('historical-dfs-data', 'points', **kwargs)