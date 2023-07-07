import get_dg_api_data as main

def get_dfs_events(**kwargs):
    """
    Returns the list of tournaments (and corresponding IDs) 
    that are available through the historical DFS data API endpoint. 
    Use this endpoint to fill the event_id and year query parameters 
    in the DFS Points endpoint.
    
    Required API parameters: 'file_format', 'key'
     
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    return main.get_dg_api_data('historical-dfs-data', 'event-list', **kwargs)


def get_dfs_data(**kwargs):
    """
    Returns salaries and ownerships alongside event-level finish, 
    hole, and bonus scoring for PGA and European Tour events.
    Data corresponds to DFS Archive page.
    
    Required API parameters: 'tour','event_id','year','file_format', 'key'
    Optional API parameters: 'site'
     
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    return main.get_dg_api_data('historical-dfs-data', 'points', **kwargs)