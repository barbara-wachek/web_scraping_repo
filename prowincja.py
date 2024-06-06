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
#%%

def get_links(url, czy_aktualnosci=False):
    # url = "https://prowincja.art.pl/category/literatura/"
    # url = "https://prowincja.art.pl/category/literatura/page/11/"

    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [] 
    articles = [e['href'] for e in soup.select('.th-readmore')]    
    links.extend(articles)
    
    next_page = soup.find('a', class_='next page-numbers')
    
    while next_page:
        new_url = next_page['href']
        
        html_text = requests.get(new_url).text
        soup = BeautifulSoup(html_text, "html.parser")
        
        articles = [e['href'] for e in soup.select('.th-readmore')]
        links.extend(articles)
        
        next_page = soup.find('a', class_='next page-numbers')
        
    return links

def dictionary_of_article(link):
    # link = article_links[0]
    # link = 'https://instytutksiazki.pl/literatura,8,recenzje,25,wszystkie,0,polnoc-i-poludnie,156.html'
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = soup.find('time', {'class': 'entry-date published'})['datetime'][:10]
       
        content_of_article = soup.find('div', class_='entry-content')
        
        try:
            tags = ' | '.join([e.text for e in soup.find_all('a', {'rel': 'tag'})])
        except AttributeError:
            tags = None
        
        author = soup.find('strong', {'style': 'font-size: 1rem;'})
        if author:
            author = author.text
        else: author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article]).strip()
        
        title_of_article = soup.find('h1', class_='entry-title')
        
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'prowincja' not in el['href']]))
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
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except (TypeError, requests.exceptions.ConnectTimeout):
        errors.append(link)
#%% main
article_links = get_links('https://prowincja.art.pl/category/literatura/')

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))   

with open(f'data/prowincja_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/prowincja_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/prowincja_{datetime.today().date()}.xlsx", f'data/prowincja_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  













