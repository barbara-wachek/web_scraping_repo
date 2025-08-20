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
        
        links = [{x.a.get('href'):category} for x in soup.find_all('article')]
        
        article_links.extend(links)
        
    
    return article_links

       

def dictionary_of_article(article_link, category):    
    
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
    
    
    try:
        date_of_publication = soup.find('time')['datetime'][:10]
    except:
        date_of_publication = None
    
    try:
        author = " | ".join([x.get_text(strip=True) for x in soup.find('div', class_='td-post-author-name').find_all('a')])
    except:
        author = None
    

    try:
        title_of_article = soup.find('h1', class_='entry-title').text
    except: 
        title_of_article = None
        
    try:
        category = " | ".join([x.text for x in soup.find('li', class_='entry-category').find_all('a')])
    except:
        category = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='td-post-source-tags').find_all('a')])
    except:
        tags = None
        
        
    
    article = soup.find('div', class_='td-post-content tagdiv-type')
    
    try:
        text_of_article = " ".join([x.get_text(strip=True) for x in article.find_all('p')])
    except:
        text_of_article = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'przegladdziennikarski', x)])
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
    
    all_results.append(dictionary_of_article)
    
    return all_results
 
#%% main

list_of_category_pages = ['https://kulturaupodstaw.pl/literatura/page/', 'https://kulturaupodstaw.pl/teatr/page/']


article_links = []
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_article_links, list_of_category_pages),total=len(list_of_category_pages)))



all_results = []
with ThreadPoolExecutor() as executor:
    results = list(tqdm(executor.map(lambda args: dictionary_of_article(*args),
                                     article_links),
                        total=len(article_links)))
    all_results.extend(results)






# all_results = []   
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(dictionary_of_article, article_links),total=len(article_links)))

  
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/przegladdziennikarski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/przegladdziennikarski_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    