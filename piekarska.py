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
def get_links_of_pages(site_url):
    # site_url = 'http://blog.piekarska.com.pl/'
    html_text = requests.get(site_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    no_of_pages = max([int(e.text) for e in soup.find_all('a', class_='page-numbers') if e.text.isnumeric()])
    return [f"http://blog.piekarska.com.pl/?paged={e}" for e in range(1, no_of_pages+1)]

def get_links_of_articles(url):
    # url = list_of_pages[0]
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    articles_links.extend([e['href'] for e in soup.select('.entry-title a')])
    
def dictionary_of_article(link):
    # link = 'http://blog.piekarska.com.pl/?p=1019'

    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        try:
            date_of_publication = soup.find('time', class_='entry-date published')['datetime'][:10]
        except TypeError:
            date_of_publication = soup.find('time', class_='entry-date published updated')['datetime'][:10]
        
        content_of_article = soup.find('div', class_='pf-content')
           
        tags = soup.find_all('a', rel='tag')
        if tags:
            tags = ' | '.join([e.text.strip() for e in tags])
        else: tags = None
        
        category = soup.find_all('a', rel='category')
        if category:
            category = ' | '.join([e.text.strip() for e in category])
        else: category = None
        
        author = soup.find('span', class_='author vcard').find('a', class_='url fn n')
        if author:
            author = author.text.strip()
        else:
            author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article.find_all('p')])
        
        title_of_article = soup.find('h1', class_='post-title entry-title').find('a', rel='bookmark')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
        
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img') if 'printfriendly.com' not in x['src']])
        except KeyError:
            photos_links = None
            
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub]))
            if sorted(external_links.split(' | ')) == sorted(photos_links.split(' | ')):
                external_links = None
        except KeyError:
            external_links = None
    
        dictionary_of_article = {'Link': link,
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Kategoria': category,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links,
                                 'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False}
    
        all_results.append(dictionary_of_article)
    except:
        errors.append(link)
    
#%% main
page_links = get_links_of_pages('http://blog.piekarska.com.pl/')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_articles, page_links),total=len(page_links)))

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, errors),total=len(errors)))   

with open(f'data/piekarska_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/piekarska_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/piekarska_{datetime.today().date()}.xlsx", f'data/piekarska_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















