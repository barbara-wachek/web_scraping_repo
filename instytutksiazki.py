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
    # url = instytutksiazki_podstrony.get('nowości')
    # url = "https://instytutksiazki.pl/literatura,8,nowosci-wydawnicze,21.html?page=2"
    # url = instytutksiazki_podstrony.get('nocny stolik')
    # czy_aktualnosci = False
    if czy_aktualnosci == False:
        selector = '.font-fira a'
    else: selector = '.font-yesevaone'
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")
    
    links = []    
    if czy_aktualnosci == False:
        articles = [e['href'] for e in soup.select(selector)]
    else:
        articles = set([e['href'] for e in soup.find_all('a', href=True, class_=False, text=False) if e['href'].startswith('aktualnosci,2,')])
    links.extend(articles)        
    
    active_page = int(soup.find('li', class_='active').text)
    new_url = f"{url}?page={active_page+1}"
    
    while len(BeautifulSoup(requests.get(new_url).text, "html.parser").select(selector)) > 0:
        html_text = requests.get(new_url).text
        soup = BeautifulSoup(html_text, "html.parser")
           
        if czy_aktualnosci == False:
            articles = [e['href'] for e in soup.select(selector)]
        else:
            articles = set([e['href'] for e in soup.find_all('a', href=True, class_=False, text=False) if e['href'].startswith('aktualnosci,2,')])
        links.extend(articles)        
        
        active_page = int(soup.find('li', class_='active').text)
        new_url = f"{url}?page={active_page+1}"
        print(active_page)
    return links

def dictionary_of_article(link):
    # link = article_links_total[555]
    # link = 'https://instytutksiazki.pl/literatura,8,recenzje,25,wszystkie,0,polnoc-i-poludnie,156.html'
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = soup.find('meta', {'name': 'article:published_time'})['content'][:10]
       
        content_of_article = soup.find('div', class_='entry rte mb-40')
        
        try:
            tags = ' | '.join([e.text for e in soup.find('div', class_='tags mb-30').find_all('a')])
        except AttributeError:
            tags = None
        
        author = [e.find('strong').text for e in content_of_article.find_all('p') if e.find('strong')]

        author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article])
        
        title_of_article = soup.find('h2', class_='h4 font-fira bold')
        
        if not title_of_article:
            title_of_article = soup.find('h2', class_='title h4 font-yesevaone')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'instytutksiazki' not in el['href']]))
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
instytutksiazki_podstrony = {'nowości': "https://instytutksiazki.pl/literatura,8,nowosci-wydawnicze,21.html",
                             'recenzje': "https://instytutksiazki.pl/literatura,8,recenzje,25.html",
                             'aktualności': "https://instytutksiazki.pl/aktualnosci,2.html"}
       
article_links1 = get_links(instytutksiazki_podstrony.get('nowości'))
article_links2 = get_links(instytutksiazki_podstrony.get('recenzje'))
article_links3 = get_links(instytutksiazki_podstrony.get('aktualności'), czy_aktualnosci=True)

article_links_total = [f"https://instytutksiazki.pl/{e}" for e in set(article_links1 + article_links2 + article_links3)]

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links_total),total=len(article_links_total)))   

with open(f'data/instytutksiazki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/instytutksiazki_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/instytutksiazki_{datetime.today().date()}.xlsx", f'data/instytutksiazki_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  













