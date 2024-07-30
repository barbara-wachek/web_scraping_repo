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
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def
def process_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    response = requests.get(article_link)
    if response.status_code != 200:
        return
    
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    if not soup.find('div', class_='artykul'):
        return

    # title
    if title_of_article := soup.find('meta', property='og:title'):
        title_of_article = title_of_article['content']
    else:
        title_of_article = None

    # author
    author = None
        
    # date
    if date := soup.find('meta', property='article:modified_time'):
        date = date['content']
        date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
        date = date.strftime('%Y-%m-%d')
    else:
        date = None
        
    # section
    if section := soup.find('meta', property='article:section'):
        section = section['content']
    else:
        section = None

    # decription
    if descripton := soup.find('meta', property='og:description'):
        descripton = descripton['content']
    else:
        descripton = None    
    
    # content
    if art_content := soup.find('div', class_='artykul'):
        text_of_article = ' '.join([p.text for p in art_content.find_all('p')])
        article = text_of_article.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Sekcja': section,
                             'Opis': descripton,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
sitemap_url = 'https://rozdzielchleb.pl/wp-sitemap-posts-post-1.xml'
articles_links = process_sitemap(sitemap_url)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/rozdzielczoscchleba_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/rozdzielczoscchleba_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/rozdzielczoscchleba_{datetime.today().date()}.xlsx", f'data/rozdzielczoscchleba_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  