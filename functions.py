#%% import
from __future__ import unicode_literals
import regex as re
import time
from datetime import datetime
from time import mktime
import requests
from bs4 import BeautifulSoup


#%% functions

#Funkcja zmieniąjąca format daty z "12 października 2002" na "2002-10-12"
def date_change_format_short(date_of_publication):
    date = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s[\w]*\s[\w]*)', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)
    
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    return new_date

#Funkcja zmieniąjąca format daty z "wtorek, 12 października 2002" na "2002-10-12"
def date_change_format_long(date_of_publication):
    # date_of_publication = 'niedziela, 14, sierpień, 2022'
    date = re.sub(r'(.*\,\s)(\d{1,2}\,?\s)(.*)(\s\d{4}\,?)', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12", "styczeń": "01", "luty": "02", "marzec": "03", "kwiecień": "04", "maj": "05", "czerwiec": "06", "lipiec": "07", "sierpień": "08", "wrzesień": "09", "październik": "10", "listopad": "11", "grudzień": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v).replace(',', '')
    
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    return new_date

#Funkcja do scrapowania linków z podanej strony sitemap
def get_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links




def date_change_new_function(date_of_publication): 
    lookup_table = {
        "styczeń": "01", "stycznia": "01",
        "luty": "02", "lutego": "02",
        "marzec": "03", "marca": "03",
        "kwiecień": "04", "kwietnia": "04",
        "maj": "05", "maja": "05",
        "czerwiec": "06", "czerwca": "06",
        "lipiec": "07", "lipca": "07",
        "sierpień": "08", "sierpnia": "08",
        "wrzesień": "09", "września": "09",
        "październik": "10", "października": "10",
        "listopad": "11", "listopada": "11",
        "grudzień": "12", "grudnia": "12"
    }

    match = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_of_publication.strip())
    if not match:
        return None  # lub np. raise ValueError("Niepoprawny format daty")
    
    day, month_name, year = match.groups()
    month = lookup_table.get(month_name.lower())
    if not month:
        return None  # lub np. raise ValueError("Nieznany miesiąc")
    
    return f"{year}-{month}-{int(day):02d}"
