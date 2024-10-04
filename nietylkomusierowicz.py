#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

#%% def    

def get_articles_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    articles_links = [e.text for e in soup.find_all('loc')]
    return articles_links
 
   
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    author = None #brak informacji w artykule i na stronie o autorze

    try:
        title_of_article = soup.find('h1', class_='entry-title').text
    except AttributeError:
        title_of_article = None
    try:
        date_of_publication = re.search(r'\d{4}-\d{2}-\d{2}', soup.find('time', class_='entry-date')['datetime']).group(0)
    except (AttributeError, TypeError):
        date_of_publication = None
    
    
    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except AttributeError:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('footer', class_='entry-meta').find_all('a', {'rel':'tag'})])
    except AttributeError:
        tags = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'nietylkomusierowicz', x)])
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
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Linki do zdjęć': photos_links}

    all_results.append(dictionary_of_article)
    
 
#%% main
articles_links =  get_articles_links('https://nietylkomusierowicz.wordpress.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
    
with open(f'data\\nietylkomusierowicz_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data\\nietylkomusierowicz_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    