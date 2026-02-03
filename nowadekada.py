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

def get_article_links(category_link):
    # category_link = 'https://nowadekada.pl/category/rozmowy/'
    all_links = []
    page = 1

    while True:
        url = category_link if page == 1 else category_link.rstrip('/') + f'/page/{page}/'
        response = requests.get(url)

        if response.status_code != 200:
            # Koniec stronicowania, nie traktujemy jako błąd
            break

        soup = BeautifulSoup(response.text, 'lxml')
        items = soup.find_all('div', class_='entry-header')  # <- dopasuj do struktury strony
        if not items:
            break

        links = [e.find('a', class_='entry-read-time')['href'] for e in items if e.find('a', class_='entry-read-time')]
        all_links.extend(links)
        page += 1

    return all_links


def extract_article_authors(title):
    title = title.strip()
    authors = []

    # RECENZUJE / RECENZUJĄ
    m = re.match(r'RECENZUJ(?:E|Ą):\s*(.+)', title)
    if m:
        # rozdzielamy po "i" w przypadku kilku autorów
        authors = [a.strip() for a in re.split(r'\si\s', m.group(1).strip())]

    # Z ... rozmawia ...
    elif 'rozmawia' in title:
        m = re.search(r'rozmawia\s+([A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\s]+)', title)
        if m:
            authors = [m.group(1).strip()]

    # wzorzec „o Tytuł” → autor przed " o "
    elif ' o ' in title:
        m = re.match(r'([A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż]+(?:\s[A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż]+)*)\s+o\s+.*', title)
        if m:
            authors = [m.group(1).strip()]

    # wzorzec z separatorem | → wieloautor dla tego samego dzieła
    elif '|' in title or '[' in title:
        authors_part = re.split(r'\[', title)[0]
        authors = [a.strip() for a in authors_part.split('|') if a.strip()]

    return authors  # zwracamy listę autorów





def dictionary_of_article(article_link):  
    
    # article_link = 'https://nowadekada.pl/andrzej-mencwel-malgorzata-szumna-marta-wyka-tamten-swiat/'
    # article_link = 'https://nowadekada.pl/olga-szmidt-wojny-nowoczesnych-plemion-michal-pawel-markowski/'
 
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    
    try:
        title_of_article = soup.find('div', class_='entry-header').find('h2').get_text(strip=True)
    except:
        title_of_article = None
  
    try:
        category = soup.find('div', class_='entry-header').find('a', {'rel':'category tag'}).get_text(strip=True)
    except:
        category = None
  
    

    author = " | ".join(extract_article_authors(title_of_article))
     
    try:
        pattern = re.compile(r'„([^”]+)”')
            
        title_of_masterpiece = " | ".join(pattern.findall(title_of_article))
    
    except:
        title_of_masterpiece = None
    
    
    
    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
          
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'nowadekada', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        

        
    dictionary_of_article = {'Link': article_link,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł dzieła': title_of_masterpiece, 
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main
category_links = ['https://nowadekada.pl/category/konstelacje/', 'https://nowadekada.pl/category/recencje/', 'https://nowadekada.pl/category/rozmowy/']


all_links = []
with ThreadPoolExecutor() as executor:
    for links in tqdm(executor.map(get_article_links, category_links), total=len(category_links)):
        all_links.extend(links)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_links),total=len(all_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()


df.to_json(f'data/nowadekada_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/nowadekada_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   