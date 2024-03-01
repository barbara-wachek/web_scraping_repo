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
from functions import date_change_format_short

#%% def
def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'http://wakat.sdk.pl/sitemap_index.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if '/post-' in x.text]
    art_links = []
    for link in tqdm(links):
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, "html.parser")
        site_links = [e.text for e in soup.find_all('loc')]
        art_links.extend(site_links)  
    return art_links

def dictionary_of_article(link):
    # link = links[11]
    # link = 'http://wakat.sdk.pl/powstanie-koalicji-czasopism-oraz-grupy-roboczej-ds-programu-promocja-literatury-i-czytelnictwa-priorytet-3-czasopisma-przy-mkidn/'

    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = soup.find('meta', {'property': 'article:published_time'})['content'][:10]
    
    content_of_article = soup.find('div', class_='tresc alfa')
    if not content_of_article:
        content_of_article = soup.find('div', class_='col9 tresc alfa')
    
    tags = soup.find('div', class_='kategoria').find('p', class_='nazwa')
    if tags:
        tags = tags.text.strip()
    else: tags = None
    
    try:
        author = soup.find('div', class_='autor').find('p', class_='nazwisko')
        if author:
            author = author.text.strip()
        else:
            author = None
    except AttributeError:
        author = None
    
    text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article.find_all('p')])
    
    title_of_article = soup.find('div', class_='col12 tresc-tytul alfa omega')
 
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if not any(ele in el['href'] for ele in ['ftn', 'wakat.sdk'])]))
    except KeyError:
        external_links = None
    
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
        photos_links = None

    dictionary_of_article = {'Link': link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tagi': tags,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe') if 'facebook.com/plugins' not in x['src']] else False}

    all_results.append(dictionary_of_article)
    
#%% main
articles_links = get_links_of_sitemap('http://wakat.sdk.pl/sitemap_index.xml')
articles_links = [e for e in articles_links if e != 'http://wakat.sdk.pl/']

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/wakat_sdk_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/wakat_sdk_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/wakat_sdk_{datetime.today().date()}.xlsx", f'data/wakat_sdk_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















