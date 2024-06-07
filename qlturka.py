#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% def

def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'https://qlturka.pl/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if 'post-sitemap' in x.text]
    article_links = []
    for link in links:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, "html.parser")
        article_links.extend([x.text for x in soup.find_all('loc')]) 
    return article_links

def dictionary_of_article(link):
    # link = articles_links[10]
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = soup.find('meta', {'property': 'article:published_time'})['content'][:10]
       
        content_of_article = soup.find('div', class_='entry-content clr')
                
        tags = '|'.join(set([e.text for e in soup.find_all('a', {'rel': 'tag'})]))
        
        author = '|'.join([e.text for e in soup.find_all('a', {'rel': 'author'})])
        
        if not author:
            author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article]).strip()
        
        title_of_article = soup.find('h1', {'style': 'font-size: 1.5em; margin-bottom: 0;'})
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'magazynwizje' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link,
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except (TypeError, requests.exceptions.ConnectTimeout):
        errors.append(link)
    
#%% main
sitemap_links = ['https://qlturka.pl/category/recenzje/ksiazki/',
                 'https://qlturka.pl/category/recenzje/film/',
                 'https://qlturka.pl/category/recenzje/teatr/',
                 'https://qlturka.pl/category/artykuly/teatr-artykuly/',
                 'https://qlturka.pl/category/artykuly/ksiazki-artykuly/',
                 'https://qlturka.pl/category/wywiady/ksiazki-wywiady/',
                 'https://qlturka.pl/category/wywiady/teatr-wywiady/',
                 'https://qlturka.pl/category/wywiady/film-wywiady/',
                 'https://qlturka.pl/category/artykuly/film-artykuly/',
                 'https://qlturka.pl/category/aktualnosci/teatr-aktualnosci/',
                 'https://qlturka.pl/category/aktualnosci/ksiazki-aktualnosci/',
                 'https://qlturka.pl/category/aktualnosci/film-aktualnosci/']

#pomysł -- pobrać wszystkie a przesiewać przez kategorie: film, ksiazki, teatr


articles_links = get_links_of_sitemap('https://qlturka.pl/sitemap.xml')

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/magazynwizje_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/magazynwizje_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/magazynwizje_{datetime.today().date()}.xlsx", f'data/magazynwizje_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















