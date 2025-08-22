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
from functions import date_change_format_short

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By



#%% def    
def get_article_links(link):
    # Lista na wszystkie linki do artykułów
    # link = 'https://kulturaupodstaw.pl/teatr/page/'
    
    
    format_link = r'(https\:\/\/kulturaupodstaw\.pl\/)([a-z]*)(/page/)'
    category = re.search(format_link, link).group(2)
    

    for page in range(1, 48):
       
        page_link = link + str(page)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Accept-Language': 'pl-PL,pl;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://przegladdziennikarski.pl/'
        }
        
        response = requests.get(page_link, headers=headers, allow_redirects=True)
        response.encoding = 'utf-8'
        
        while 'Error 503' in response:
            time.sleep(2)
            response = requests.get(page_link, headers=headers, allow_redirects=True)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = [(x.a.get('href'), category) for x in soup.find_all('article')]
        
        article_links.extend(links)
        
    
    return article_links

       

def dictionary_of_article(article_link, category):    
    
    # article_link = 'https://kulturaupodstaw.pl/odciecie-od-wlasnych-emocji-rozmowa-z-arkiem-kowalikiem/'
    
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
                'Accept-Language': 'pl-PL,pl;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': 'https://przegladdziennikarski.pl/'
            }
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    josefin_elements = soup.find_all('p', class_='josefin post-info')
    
    try:
        new_date = " | ".join([x.text[13:] for x in josefin_elements if x.text.startswith('Opu')]) 
        date_of_publication = date_change_format_short(new_date)
    except:
        date_of_publication = None
    
    
    try:
        author = " | ".join([x.get_text(strip=True)[7:] for x in josefin_elements if x.text.startswith('tekst')])
    except:
        author = None
    

    try:
        title_of_article = soup.find('h1').text
    except: 
        title_of_article = None
        
        
    
    article = soup.find('div', class_=re.compile(r'^single-post'))
    
    try:
        tags = " | ".join([x.text for x in article.find_all('a') if re.search(r'^https\:\/\/kulturaupodstaw\.pl\/tag\/.*', x.get('href'))])
    except:
        tags = None
        
    
    try:
        text_of_article = article.get_text(strip=True)
    except:
        text_of_article = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'kulturaupodstaw', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool(article and article.find_all('img')),
                             'Filmy': bool(article and article.find_all('iframe')),
                             'Linki do zdjęć': photos_links
                             }
    
    return dictionary_of_article
 
#%% main

list_of_category_pages = ['https://kulturaupodstaw.pl/literatura/page/', 'https://kulturaupodstaw.pl/teatr/page/']


article_links = []
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_article_links, list_of_category_pages),total=len(list_of_category_pages)))

# pairs = [(list(d.keys())[0], list(d.values())[0]) for d in article_links]

all_results = []
with ThreadPoolExecutor() as executor:
    all_results = list(
        tqdm(
            executor.map(lambda args: dictionary_of_article(*args), article_links),
            total=len(article_links)
        )
    )

  
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/kulturaupodstaw_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/kulturaupodstaw_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    