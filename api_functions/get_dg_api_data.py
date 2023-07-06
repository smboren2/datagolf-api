import requests 
from os.path import join
from pandas import read_csv
from io import StringIO
from itertools import product
import time


def get_dg_api_data(primary_path: str, secondary_path: str, **kwargs):
    """Datagolf API call

    Generates an API call to the Datagolf API based on the parameters in the
    query string. An API key is required for all calls, while additional
    parameters may be required depending on the endpoint.

    See https://datagolf.com/api-access for documentation.

    Args:
        primary_path: the primary endpoint category
        secondary_path: the secondary endpoint category, if applicable
        **kwargs: Keyword arguments, best passed as a dictionary of API parameters.

    Returns:
        depending on file_format requested, json or csv
    """

    url = join('https://feeds.datagolf.com', primary_path, secondary_path)

    response = requests.get(url, kwargs)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return "Error: " + str(e)

    if response.status_code != 200 or response.content.decode('utf-8') == '\n':
        data = 'skip'
    else: 
        data = read_csv(StringIO(response.content.decode('utf-8')))
        
    return data
