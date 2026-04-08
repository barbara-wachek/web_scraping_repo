#%%import
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



#%%def
def get_article_links(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc') if re.match(r"https\:\/\/tlusty\.substack\.com\/p\/.*", e.text) ]
    return links
    
    
def dictionary_of_article(article_link):

    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
 
    try: 
        script_type = soup.find('script', {'type': 'application/ld+json'})
        date_of_publication = json.loads(script_type.string)['datePublished'][:10]
    except:
        date_of_publication = None
       
        
    try:
        title_of_article = soup.find('h1', {'dir': 'auto'}).text
    except:
        title_of_article = None
               
   
    
    article = soup.find('div', class_='available-content')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except: 
        text_of_article = None
   
    author = None
    try:
        italic_elements = [x.text for x in article.find_all('em')]
        
        author_regex = re.compile(r'^[A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+\s+[A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+$')

        for x in reversed(italic_elements):  
            if author_regex.match(x):
                author = x
                break
    except:
        author = None
        
    related_masterpiece = None    
    try:
        for p in reversed(article.find_all("p")):
            text = p.get_text(" ", strip=True)
        
            if p.find("em") and re.search(r'\b(19|20)\d{2}\b', text):
                related_masterpiece = text
                break
                
    except:
        related_masterpiece = None
        
        

    try:
        author_of_book = re.match(r"^([A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+\s+[A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+),", related_masterpiece).group(1)
    except:
        author_of_book = None
    
    try:
        title_of_book = re.match(r"^[A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+(?:\s+[A-ZŁŚŻŹĆŃÓ][a-ząćęłńóśźż]+)*,\s*(.+?)\s*,", related_masterpiece).group(1)
    except:
        title_of_book = None
        
    try:
        year_of_publication = re.search(r'\b(19|20)\d{2}\b', related_masterpiece).group()
    except:
        year_of_publication = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'substack', x)])
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
                             'Tytuł książki': title_of_book, 
                             'Autor książki': author_of_book,
                             'Rok wydania': year_of_publication, 
                             'Opis książki': related_masterpiece, 
                             "Tekst artykułu": text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if getattr(article, 'find_all', lambda x: [])('img') else False,
                             'Filmy': True if getattr(article, 'find_all', lambda x: [])('iframe') else False,
                             'Linki do zdjęć': photos_links
                             }
 
    all_results.append(dictionary_of_article)

#%% main

article_links = get_article_links('https://tlusty.substack.com/sitemap.xml')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
  
    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/tlustydruk_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)       
        
with pd.ExcelWriter(f"data/tlustydruk_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
    df.to_excel(writer, 'Posts', index=False)
   
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
            