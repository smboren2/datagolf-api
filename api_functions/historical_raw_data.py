import get_dg_api_data as main

def get_round_stroke_events(**kwargs):
    """
    Returns the list of tournaments (and corresponding IDs) that are available 
    through the historical raw data API endpoint. Use this endpoint to fill the 
    event_id and year query parameters in the Round Scoring & Strokes Gained endpoint.
     
    Required API parameters: 'file_format', 'key'
    Optional API parameters: 'tour' 
     
    Args:
       **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
     
    Returns: 
       csv
    """
    return main.get_dg_api_data('historical-raw-data', 'event-list', **kwargs)


def get_round_stroke_data(**kwargs):
    """
    Returns round-level scoring, traditional stats, strokes-gained, 
    and tee time data across 22 global tours. Data corresponds to Raw Data Archive page. 
    For variable/field definitions, general notes, and changelog 
    check https://datagolf.com/raw-data-notes.
    
    Required API parameters: 'tour','event_id','year','file_format', 'key'
     
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.
    
    Returns: 
        csv
    """
    return main.get_dg_api_data('historical-raw-data', 'rounds', **kwargs)