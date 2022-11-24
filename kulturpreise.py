#%%import
#from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

#%%def

def get_all_subpages_links(link):    
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    list_of_subpages = [x.a['href'] for x in soup.find_all('td', class_='als') if x.find('a')]
    
    return list_of_subpages
   
def get_links_of_donors(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    links = [x.a['href'] for x in soup.find_all('li') if x.find('a')]
    for link in links:
        if re.search(r'http:\/\/www\.kulturpreise\.de\/web\/register_stifter\.php\?stifter=.*', link):
            links_of_donors.append(link)
    
    return links_of_donors
    
def get_links_of_awards_from_donors(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.a['href'] for x in soup.find_all('tr', re.compile('even|odd'))]
    links_of_awards_from_donors.extend(links)

    
def dictionary_of_award(link):
    # link = 'http://www.kulturpreise.de/web/preise_info.php?preisd_id=5629'
    # link = 'http://www.kulturpreise.de/web/preise_info.php?preisd_id=4019'
    # link = 'http://www.kulturpreise.de/web/preise_info.php?preisd_id=2899'
    
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    name_of_award = soup.find('h1', class_='title')
    if name_of_award:
        name_of_award = soup.find('h1', class_='title').text.strip()
    else:
        name_of_award = None
        
    subtitle_of_award = soup.find('p', class_='subtitle')
    if subtitle_of_award:
        subtitle_of_award = soup.find('p', class_='subtitle').text.strip()
    else:
        subtitle_of_award = None
   
    founded_year = None
    total_endowment = None
    place_of_award = None
    currency = None
    
    if subtitle_of_award != None:
        if re.search(r'^(Gründungsjahr\:\s)(\d{4})\,\s(Ort der Verleihung\:\s)(\p{L}*)\,?\s?(Gesamtdotierung\:\s)?(\d\d?\.?\d?\d?\d?\d?\d?)?(.*)?', subtitle_of_award):
            founded_year = re.sub(r'^(Gründungsjahr\:\s)(\d{4})\,\s(Ort der Verleihung\:\s)(\p{L}*)\,?\s?(Gesamtdotierung\:\s)?(\d\d?\.?\d?\d?\d?\d?\d?)?(.*)?', r'\2', subtitle_of_award)
            place_of_award = re.sub(r'^(Gründungsjahr\:\s)(\d{4})\,\s(Ort der Verleihung\:\s)(\p{L}*)\,?\s?(Gesamtdotierung\:\s)?(\d\d?\.?\d?\d?\d?\d?\d?)?(.*)?', r'\4', subtitle_of_award)
            total_endowment = re.sub(r'^(Gründungsjahr\:\s)(\d{4})\,\s(Ort der Verleihung\:\s)(\p{L}*)\,?\s?(Gesamtdotierung\:\s)?(\d\d?\.?\d?\d?\d?\d?\d?)?(.*)?', r'\6', subtitle_of_award)
            currency = re.sub(r'^(Gründungsjahr\:\s)(\d{4})\,\s(Ort der Verleihung\:\s)(\p{L}*)\,?\s?(Gesamtdotierung\:\s)?(\d\d?\.?\d?\d?\d?\d?\d?)?(.*)?', r'\7', subtitle_of_award).strip()
       
        
    informations_about_award = soup.find('tbody') 
    if informations_about_award:
       names_of_columns = [x.td.text for x in informations_about_award.find_all('tr') if re.search(r'\p{L}*\:', x.td.text)]
       values_of_columns = [x.find_all('td')[1].text.strip().replace('\xa0', '') for x in informations_about_award.find_all('tr') if x.find_all('td')[1].text != '\xa0']
       
     
    dictionary_of_other_informations = {names_of_columns[i]: values_of_columns[i] for i in range(len(names_of_columns))}


    dictionary_of_award = {'Name of award': name_of_award,
                           'Founded year': founded_year,
                           'Place of award': place_of_award,
                           'Total endowment': total_endowment,
                           'Currency': currency,
                           'Other informations': dictionary_of_other_informations
                           }
    all_results.append(dictionary_of_award)            
        
    
#%%main


get_all_subpages_links('http://www.kulturpreise.de/web/register_stifter.php?group=A')


links_of_donors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_donors, list_of_subpages), total=len(list_of_subpages)))       


links_of_awards_from_donors = [] #with duplicates
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_awards_from_donors, links_of_donors), total=len(links_of_donors)))    


links_of_awards_from_donors_without_duplicates = list(set(links_of_awards_from_donors))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_award, links_of_awards_from_donors_without_duplicates), total=len(links_of_awards_from_donors_without_duplicates)))    

df = pd.DataFrame(all_results)


#2022-11-24
#Pomyslec jeszcze jakie informacje zeskrobac ze strony kazdej z nagrod
#Może wyciaganc ID nagrody? w celu uporządkowania informacji?












