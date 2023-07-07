import get_dg_api_data as main
import time

def get_historical_odds_events(**kwargs):
    """
    Returns the list of tournaments (and corresponding IDs) that are 
    available through the historical odds/predictions endpoints. 
    Use this endpoint to fill the event_id and year query parameters in 
    the Archived Predictions, Historical Outrights, and Historical Matchups 
    & 3-Balls endpoints.
    
    Required API parameters: 'file_format', 'key'
     
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    return main.get_dg_api_data('historical-odds', 'event-list', **kwargs)


def get_historical_odds_outrights(**kwargs):
    """
    Returns opening and closing lines in various markets 
    (win, top 5, make cut, etc.) at 11 sportsbooks. 
    Bet outcomes also included.
    
    Required API parameters: 'market','book','file_format','key'
    Optional API parameters: 'tour','event_id','year','odds_format'
     
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    time.sleep(1)
    return main.get_dg_api_data('historical-odds', 'outrights', **kwargs)


def get_historical_odds_matchups(**kwargs):
    """
    Returns opening and closing lines for tournament match-ups, 
    round match-ups, and 3-balls at 12 sportsbooks. Bet outcomes also included.
    
    Required API parameters: 'book','file_format','key'
    Optional API parameters: 'tour','event_id','year','odds_format'
    
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    time.sleep(1)
    return main.get_dg_api_data('historical-odds', 'matchups', **kwargs)