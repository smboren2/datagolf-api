import pandas as pd
from os import getenv

from api import general_use as gen

def get_field(request):
    dg_api = getenv("DG_API")
    field = gen.get_field_updates(**{'file_format':'csv', 'key': dg_api})

    print(field.head(5))
    
    return "OK"