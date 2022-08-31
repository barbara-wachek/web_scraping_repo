from __future__ import unicode_literals
import regex as re
import time
from datetime import datetime
from time import mktime


#%% Funkcja zmieniąjąca format daty z "12 października 2002" na "2002-10-12"



def date_change_format_short(date_of_publication):
    date = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s[\w]*\s[\w]*)', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)
    
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    return new_date


