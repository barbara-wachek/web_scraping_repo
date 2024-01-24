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
    
    if not soup.find('article'):
        return
    
    if title_of_article := soup.find('h1', class_='entry-title'):
        title_of_article = title_of_article.text
    else:
        title_of_article = None

    if art_meta := soup.find('div', class_='entry-meta'):
        date_of_publication = art_meta.find('span', class_='posted-on').find('time')['datetime']
        date_of_publication = datetime.fromisoformat(date_of_publication)
        date_of_publication = date_of_publication.strftime('%Y-%m-%d')
        
        author_of_article = art_meta.find('span', class_='byline').text.strip()
        
        category_of_article = art_meta.find('span', class_='cat-links').text.strip()
    else: 
        date_of_publication, author_of_article, category_of_article = None, None, None
    
    
    if art_content := soup.find('div', class_='entry-content'):
        text_of_article = art_content
        article = text_of_article.text.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author_of_article,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category_of_article,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
articles_links = []
start_url = "https://remigiusz-grzela.pl/?paged="
i = 0
while True:
    i+=1
    print('Page: ', i)
    response = requests.get(start_url + str(i))
    if response.status_code != 200:
        break
    page_text = response.text
    soup = BeautifulSoup(page_text, 'lxml')
    art_links = [header.find('a')['href'] for header in soup.find_all('header', class_='entry-header')]
    articles_links.extend(art_links)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/remigiuszgrzela_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/remigiuszgrzela_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/remigiuszgrzela_{datetime.today().date()}.xlsx", f'data/remigiuszgrzela_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
