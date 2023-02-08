#%% import
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
from functions import date_change_format_long, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive



#%% def
    
def get_article_pages(link):    
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)   
    
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
  
    author = "Maciej Robert"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    date_of_publication = soup.find('h2', class_='date-header').text
    new_date = date_change_format_long(date_of_publication)
    texts_of_article = soup.find('div', class_='post-body entry-content')
    article = texts_of_article.text.strip().replace('\n', ' ')
    tags_span = soup.find_all('span', class_='post-labels')
    tags = ' | '.join([tag.text.replace('Etykiety:', '').replace(',',' ') for tag in tags_span][0].split())
    
    try:
        author_of_book = re.findall(r'(?<=\p{Lu}\s\-\s)[\p{L}\-\s\.]*(?=\"|\„.*\"|\”)', title_of_article)[0].strip() 
    except (AttributeError, KeyError, IndexError):
        author_of_book = None
        
    try:
        title_of_book = re.sub(r'([\p{Lu}\s0-9\.\,\-\'\!\(\)\"\„\”\=\?]*)(\s\-\s)([\p{L}\-\s\.\']*)([\"|\„].*[\"|\”]?)', r'\4', title_of_article)
    except (AttributeError, KeyError, IndexError):
        title_of_book = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|bialafabryka', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book if author_of_book != None else None,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}

    all_results.append(dictionary_of_article)
    

#%% main
sitemap_links = get_links('http://czytamcentralnie.blogspot.com/sitemap.xml')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))

all_results = []    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'czytam_centralnie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)       
          
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"czytam_centralnie_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"czytam_centralnie_{datetime.today().date()}.xlsx", f'czytam_centralnie_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
       
        
       
        
       
        
       
        
       
        
       
        
         
       
