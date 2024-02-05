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
    # sitemap_link = 'https://zeszytypoetyckie.pl/mapa-strony'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.find('a')['href'] for x in soup.find_all('li')]
    links = [f"https://zeszytypoetyckie.pl{e}" for e in links if e.startswith('/') and e.count('/') > 1]
    return links

def dictionary_of_article(link):
    # link = articles_links[100]
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = None
   
    content_of_article = soup.find('td', {'valign': 'top'})
    
    tags = None
  
    author = soup.find('span', {'style': 'color:#000000;'})
    if author and author.find('strong'):
        author = author.find('strong').text
    else: author = None
    
    if content_of_article:
        text_of_article = content_of_article.text.strip().replace('\n', '')
    else:
        text_of_article = None
 
    title_of_article = soup.find('td', class_='contentheading')
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None
    
    if content_of_article:
        try:
            external_links = ' | '.join([e.find('a')['href'] for e in content_of_article.find_all('p') if e.find('a') and 'zeszytypoetyckie.pl' not in e.find('a')['href']])
        except KeyError:
            external_links = None
        
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('input', {'type': 'image'})])
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
articles_links = get_links_of_sitemap('https://zeszytypoetyckie.pl/mapa-strony')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/zeszytypoetyckie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        


df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)


with pd.ExcelWriter(f"data/zeszytypoetyckie_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/zeszytypoetyckie_{datetime.today().date()}.xlsx", f'data/zeszytypoetyckie_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















