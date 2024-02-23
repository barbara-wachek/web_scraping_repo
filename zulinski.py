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
    #sitemap_link = 'https://www.zulinski.pl/wp-sitemap-posts-post-1.xml'
    html_text = requests.get(sitemap_link, verify=False).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    
    return sitemap_links

def dictionary_of_article(link):
    # link = articles_links[100]
    html_text = requests.get(link, verify=False).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = soup.find('p', class_='postinfo').text.split(':')[-1].strip()
    date_of_publication = date_change_format_short(date_of_publication)
   
    content_of_article = soup.find_all('p')
    content_of_article = [e for e in content_of_article if e.text][1:-2]
    
    tags = ' | '.join([e.text for e in soup.find_all('a', {'rel': 'category tag'})])
    
    author = 'Leszek Żuliński'
    
    if content_of_article:
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article])
    else:
        text_of_article = None
    
    title_of_article = soup.find('a', {'rel': 'bookmark'})
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None
    
    if content_of_article:
        try:
            external_links = [el for sub in [e.find_all('p') for e in content_of_article] for el in sub]
            external_links = ' | '.join([e.find('a')['href'] for e in external_links if e.find('a') and 'zulinski.pl' not in e.find('a')['href']])
        except KeyError:
            external_links = None
        
        try:
            photos_links = [el for sub in [e.find_all('input', {'type': 'image'}) for e in content_of_article] for el in sub]
            photos_links = ' | '.join([x['src'] for x in photos_links])
        except KeyError:
            photos_links = None
    else:
        external_links = None
        photos_links = None
        
    dictionary_of_article = {'Link': link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                             }

    all_results.append(dictionary_of_article)
    
#%% main
articles_links = get_links_of_sitemap('https://www.zulinski.pl/wp-sitemap-posts-post-1.xml')
articles_links.pop(articles_links.index('https://www.zulinski.pl/polecam/'))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/zulinski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/zulinski_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
   
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/zulinski_{datetime.today().date()}.xlsx", f'data/zulinski_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



















