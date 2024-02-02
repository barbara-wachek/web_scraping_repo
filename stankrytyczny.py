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
    # sitemap_link = 'https://stankrytyczny.wordpress.com/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if re.findall(r'^https://stankrytyczny.wordpress.com/\d+', x.text)]
    return links

def dictionary_of_article(link):
    # link = 'https://stankrytyczny.wordpress.com/2016/07/16/wyszlo-z-mody-numer-specjalny-stanu-krytycznego/'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = soup.find('small').find('strong').text
    date_of_publication = f"{date_of_publication[-4:]}-{date_of_publication[3:5]}-{date_of_publication[:2]}"
   
    content_of_article = soup.find('div', class_='entry')
    
    tags = '|'.join([e.text for e in soup.find_all('a', rel='category tag')])
  
    author = soup.find('p', class_='postmetadata').find('a', rel='tag')
    if author:
        author = author.text
    else: author = None

    text_of_article = content_of_article.text.strip().replace('\n', '')
 
    title_of_article = soup.find('h2')
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join([e.find('a')['href'] for e in content_of_article.find_all('p') if e.find('a') and 'stankrytyczny' not in e.find('a')['href']])
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
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
articles_links = get_links_of_sitemap('https://stankrytyczny.wordpress.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/stankrytyczny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        


df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)


with pd.ExcelWriter(f"data/stankrytyczny_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/stankrytyczny_{datetime.today().date()}.xlsx", f'data/stankrytyczny_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















