
import requests
from bs4 import BeautifulSoup

import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time


#%%def

def get_article_links(sitemap_url): 
    html_text = requests.get(sitemap_url).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):  
    
    # article_link = 'https://miastoliteratury.com/aktualnosci/moja-corka-lubi-sprawdzac-czy-ma-rozowe-w-oku-tam-gdzie-sie-pokazuje-jedzie-mi-tu-czolg/'
    # article_link = 'https://miastoliteratury.com/literatura/euromajdan/'
    # article_link = 'https://miastoliteratury.com/aktualnosci/sladowisko-odslona-1/'
 
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    
    try:
        title_of_article = soup.find('h1').get_text(strip=True)
    except:
        title_of_article = None
  
    try:
        author = soup.find('div', class_='l-post__header__info').find('a').text
        if author.startswith('nr'):
            author = None
    except:
        author = None
        
    try:
        date_of_publication = "".join([x.text for x in soup.find('div', class_='l-post__header__info').find_all('li') if re.match(r'^\d.*', x.text)])
        date_of_publication = datetime.strptime(date_of_publication, "%d.%m.%Y").strftime("%Y-%m-%d")
    except:
        date_of_publication = None    
        

    article = soup.find('div', class_='l-article__block')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='l-tags').find_all('a')])
    except:
        tags = None
        
    try:
        issue = " | ".join([x.text for x in soup.find('div', class_='l-tags').find_all('a') if re.match(r'^nr.*', x.text)])
    except:
        issue = None
        
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'pismoludziprzelomowych', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        

        
    dictionary_of_article = {'Link': article_link,
                             'Numer': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main

article_links = get_article_links("https://miastoliteratury.com/post-sitemap.xml")
print(f"Znaleziono {len(article_links)} linków.")


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()

df.to_json(f'data/miastoliteratury_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/miastoliteratury_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   