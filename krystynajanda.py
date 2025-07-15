#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time
from functions import date_change_new_function


#%% def    

def get_monthly_links(diary_link):
    html_text = requests.get(diary_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    monthly_links = [e.get('value') for e in soup.find('select', {'name': 'archive-dropdown'}).find_all('option') if e.get('value') != '']
    return monthly_links



def dictionary_of_article(monthly_link):  
       
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(monthly_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(monthly_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    monthly_diary = soup.find_all('div',class_='sub_dziennik_item')
    results = []
    
    for element in monthly_diary:
        author = 'Krystyna Janda'
        
        try:
            day = element.find('div', class_='day').text
            month = element.find('div', class_='month').text
            year = element.find('div', class_='year').text
            
            date = day + " " + month + " " + year
            date_of_publication = date_change_new_function(date)
        except: 
            date_of_publication = None
            

        try:
            title_of_article = [x.text for x in element.find('div', class_='bd').find_all('p')][0]
        except: 
            title_of_article = None
        
        
        try:
            text_of_article = " ".join([x.text for x in element.find('div', class_='bd').find_all('p')])
        except:
            text_of_article = None

        try:
            external_links = ' | '.join([x for x in [x['href'] for x in element.find_all('a')] if not re.findall(r'krystynajanda', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        try: 
            photos_links = ' | '.join([x['src'] for x in element.find_all('img')])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None


        dictionary_of_article = {'Link': monthly_link,
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Zdjęcia/Grafika': bool(element and element.find_all('img')),
                                 'Filmy': bool(element and element.find_all('iframe')),
                                 'Linki do zdjęć': photos_links
                                 }
        
        results.append(dictionary_of_article)
    
    return results
 
#%% main

monthly_links = get_monthly_links('https://krystynajanda.pl/dziennik/')

all_results = []   
with ThreadPoolExecutor() as executor:
    for result_list in tqdm(executor.map(dictionary_of_article, monthly_links), total=len(monthly_links)):
        all_results.extend(result_list)  # Dopiero tutaj zbierasz wszystko razem
   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/krystynajanda_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/krystynajanda_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    