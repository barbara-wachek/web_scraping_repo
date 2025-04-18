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

def get_article_links(sitemap_link):
    # sitemap_link = 'https://moreleigrejpfruty.com/Archiwum,ar.html'
    html_text = requests.get(sitemap_link)
    html_text.encoding = 'utf-8'
    html_text = html_text.text
    soup = BeautifulSoup(html_text, "html.parser")
    issues = [(e.text.strip(), 'https://moreleigrejpfruty.com' + e.find('a')['href']) for e in soup.find_all('div', {'class': 'numer'})]
    links = []
    for d, i in issues:
        # d,i = issues[0]
        html_text = requests.get(i).text
        soup = BeautifulSoup(html_text, 'html.parser')
        arts = ['https://moreleigrejpfruty.com' + e.find('a')['href'] for e in soup.find_all('div', {'class': 'news'})]
        for a in arts:
            links.append((a,d))
    return links

dates_dict = {'nr 1 / wrzesień 2007': '2007-09-01',
 'nr 10 / październik 2008': '2008-10-01',
 'nr 11 / listopad 2008': '2008-11-01',
 'nr 12 /  grudzień 08 /styczeń 09': '2008-12-01',
 'nr 2 / listopad-grudzień 2007': '2007-11-01',
 'nr 3 / styczeń-luty 2008': '2008-01-01',
 'nr 4 / marzec 2008': '2008-03-01',
 'nr 5 / kwiecień 2008': '2008-04-01',
 'nr 6 / czerwiec 2008': '2008-06-01',
 'nr 7 / lipiec 2008': '2008-07-01',
 'nr 8 / sierpień 2008': '2008-08-01',
 'nr 9 / wrzesień 2008': '2008-09-01'}

def dictionary_of_article(link):
    # link = articles_links[20]
    # link = 'https://moznaprzeczytac.pl/fantasyexpo-wroclaw-13-10-2013/'
    try:
        html_text = requests.get(link[0])
        html_text.encoding = 'utf-8'
        html_text = html_text.text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = dates_dict.get(link[-1])
       
        content_of_article = soup.find('div', class_='tresc')
        
        tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')])
        
        author = soup.find('div', {'class': 'autor'}).text
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
        
        title_of_article = soup.find('h2')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'moreleigrejpfruty' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link[0],
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links,
                                 'Numer': link[-1]
                                 }
    
        all_results.append(dictionary_of_article)
    except TypeError:
        errors.append(link)
    
#%% main
articles_links = get_article_links('https://moreleigrejpfruty.com/Archiwum,ar.html')

all_results = []
# do_over = errors.copy()

errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/moreleigrejpfruty_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/moreleigrejpfruty_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/moreleigrejpfruty_{datetime.today().date()}.xlsx", f'data/moreleigrejpfruty_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















