import get_dg_api_data as main

# ------------ General Use ------------
def get_player_list(**kwargs):
    """Returns the list of players who have played on a "major tour" since 2018, or are playing on a major 
        tour this week. IDs, country, amateur status included.
    
    Required API parameters: 'file_format', 'key'
    
    Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.

    Returns: 
        csv
    """

    return main.get_dg_api_data('get-player-list', '', **kwargs)


def get_tour_schedule(**kwargs): 
    """Current season schedules for the primary tours (PGA, European, KFT). Includes event names/ids, 
        course names/ids, and location (city/country and latitude, longitude coordinates) data for select tours.
    
    Required API parameters: 'file_format', 'key'
    Optional API parameters: 'tour' 
    
     Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.

    Returns: 
        csv    
    """

    return main.get_dg_api_data('get-schedule', '', **kwargs)


def get_field_updates(**kwargs): 
    """Up-to-the-minute field updates on WDs, Monday Qualifiers, tee times, and fantasy salaries for
        PGA Tour, European Tour, and Korn Ferry Tour events. Includes data golf IDs and tour-specific
        IDs for each player in the field.
    
    Required API parameters: 'file_format', 'key'
    Optional API parameters: 'tour' 
    
     Args:
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.

    Returns: 
        csv    
    """

    return main.get_dg_api_data('field-updates', '', **kwargs)
