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

#%% def    

def get_article_links(sitemap_links):
    
    # sitemap_links = ['https://www.niezatapialna-armada.pl/wp-sitemap-posts-post-1.xml', 'https://www.niezatapialna-armada.pl/wp-sitemap-posts-page-1.xml']
    all_links = []
    for link in sitemap_links:
        
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.text for e in soup.find_all('loc')]
        all_links.extend(links)
        
    return all_links



def dictionary_of_article(article_link):  
    
    # article_link = 'https://www.niezatapialna-armada.pl/2020/06/25/84-z-kamera-wsrod-mezczyzn-czyli/'
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'  # wymuś UTF-8
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    try:
        date_of_publication = soup.find('time', class_='published')['datetime'][:10]
    except:
        date_of_publication = None   
    
    try:
        author = soup.find('span', class_='author-name').text
    except:
        author = None

    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text
    except: 
        title_of_article = None



    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = article.get_text(separator=" ", strip=True).replace('\n', '')
    except:
        text_of_article = None
    
    try:
        category = " | ".join([x.text for x in soup.find('span', class_="cat-links").find_all('a')])
    except:
        category = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='tags-links').find_all('a')])
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'niezatapialna-armada', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)
    
    
 
#%% main

article_links = get_article_links(['https://www.niezatapialna-armada.pl/wp-sitemap-posts-post-1.xml', 'https://www.niezatapialna-armada.pl/wp-sitemap-posts-page-1.xml'])
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/niezatapialnaarmada_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/niezatapialnaarmada_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    