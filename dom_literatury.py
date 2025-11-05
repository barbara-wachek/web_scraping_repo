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
def dictionary_of_article(link):
    # link = 'https://dom-literatury.pl/blog/leonora-carrington-trabka-do-sluchania'

    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = ''
        
        title_of_article = soup.find('title').text.replace(' - Dom Literatury', '')
        author = 'Łukasz Barys'
        
        content_of_article = soup.find('section')
                        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article.find('div', class_=re.compile(r'^blog_data'))]).strip()
        
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('div')] for el in sub if 'teatralia' not in el['href']]))
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
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except (AttributeError, ValueError, TypeError):
        errors.append(link)
    except IndexError:
        pass
    
#%% main
articles_links = [
    'https://dom-literatury.pl/blog/coco-mellors-kleopatra-i-frankenstein',
    'https://dom-literatury.pl/blog/grzegorz-bogdal-idzie-tu-wielki-chlopak-2',
    'https://dom-literatury.pl/blog/anna-cieplak-cialo-huty',
    'https://dom-literatury.pl/blog/li-ang-zona-rzeznika',
    'https://dom-literatury.pl/blog/ann-quin-berg',
    'https://dom-literatury.pl/blog/bryan-washington-parking',
    'https://dom-literatury.pl/blog/lidia-yuknavitch-chronologia-wody',
    'https://dom-literatury.pl/blog/nony-fernandez-strefa-mroku',
    'https://dom-literatury.pl/blog/leonora-carrington-trabka-do-sluchania',
    'https://dom-literatury.pl/blog/iddo-gefen-jerozolimska-plaza',
    ]

all_results = []

errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/dom_literatury_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/dom_literatury_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/dom_literatury_{datetime.today().date()}.xlsx", f'data/dom_literatury_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  


