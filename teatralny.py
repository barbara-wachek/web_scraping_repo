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
    # sitemap_link = 'https://teatralny.pl/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if x.text.count('/') >= 4]
    return links

# kategorie = set([e.split('/')[3] for e in links if e.count('/') >= 4])

# examples = []
# for el in kategorie:
#     for e in links:
#         if f"/{el}/" in e:
#             examples.append(e)
#             break

def dictionary_of_article(link):
    # link = examples[2]
    # link = 'https://teatralny.pl/news/pl/6'

    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        date_of_publication = soup.find_all('p', style='text-align: right;')[-1].find('em').text.split('-')
        date_of_publication = f"{date_of_publication[-1].strip()}-{date_of_publication[1].strip() if len(date_of_publication[1].strip()) == 2 else '0' + date_of_publication[1].strip()}-{date_of_publication[0].strip() if len(date_of_publication[0].strip()) == 2 else '0' + date_of_publication[0].strip()}"
    except (IndexError, AttributeError):
        date_of_publication = None
   
    content_of_article = soup.find('div', {'id': 'mcont'})
    
    if content_of_article:
    
        tags = soup.find('div', class_='tagsContetnt').find_all('a')
        if tags:
            tags = ' | '.join([e.text for e in tags])
        else: tags = None
        
        category = link.split('/')[3]
        
        author = soup.find('div', class_='authorArea').find('div').find('a')
        if author:
            author = author.text
        else:
            author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article.find_all('p')])
        
        title_of_article = soup.find('article', {'id': 'single_article'}).find('h2')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'teatralny' not in el['href']]))
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
                                 'Kategoria': category,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links,
                                 'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False}
    
        all_results.append(dictionary_of_article)
    
#%% main
articles_links = get_links_of_sitemap('https://teatralny.pl/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/teatralny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str) 

df = pd.DataFrame(all_results)
df.loc[df['Link'] == 'https://teatralny.pl/recenzje/opera-na-niedziele,681.html', 'Data publikacji'] = '2014-09-12'
df.loc[df['Link'] == 'https://teatralny.pl/recenzje/wyzwolenie,1891.html', 'Data publikacji'] = '2017-03-03'
df.loc[df['Link'] == 'https://teatralny.pl/recenzje/etos-patos-dupa-zbita,346.html', 'Data publikacji'] = '2014-03-03'
df.loc[df['Data publikacji'] == "20222-01-19", 'Data publikacji'] = '2022-01-19'

df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/teatralny_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/teatralny_{datetime.today().date()}.xlsx", f'data/teatralny_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















