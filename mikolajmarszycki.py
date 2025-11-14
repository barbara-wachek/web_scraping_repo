#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from functions import date_change_format_long


#%%def
def sitemap_web_scraping(url):
    driver = webdriver.Firefox()
    # url = "https://natemat.pl/blogi/mikolajmarszycki"
    driver.get(url)
    time.sleep(3)
    html_text = driver.page_source
    soup = BeautifulSoup(html_text, 'lxml')
    links = [f"https://natemat.pl{e['href']}" for e in soup.find_all('a', {'class': 'page-link'}) if 'blogi/mikolajmarszycki/' in e['href'] and len(e['href']) > 42]
    driver.close()
    return links  

def dictionary_of_article(article_link):
# for article_link in article_links:
    # article_link = article_links[37]
    # article_link = 'https://pismointer.wordpress.com/aktualny-numer/krytyka/rafal-rozewicz-co-pozostaje-czytajac-to-bruk-piotra-gajdy-i-trzeci-migdal-mirki-szychowiak/'
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date_of_publication = [e.text for e in soup.find_all('div', {'class': 'c-jpldWA'})][1].split(',')[0]
        date_of_publication = date_change_format_long(date_of_publication)
    except (ValueError, TypeError):
        date_of_publication = None
    
    content_of_article = soup.find('div', class_='art')
    
    author = 'Mikołaj Marszycki'
    
    title_of_article = soup.find('h1', class_='c-eTFcmV')
 
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None
    
    tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')])
    
    text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
    
    try:
        external_links = ' | '.join(set([e['href'] for e in content_of_article.find_all('div') if 'href' in e.attrs and 'natemat' not in e['href']]))
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
        photos_links = None
        
    dictionary_of_article = {'Link' : article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tagi': tags if tags != '' else None,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links} 
     
    all_results.append(dictionary_of_article)    


#%%main

article_links = sitemap_web_scraping('https://natemat.pl/blogi/mikolajmarszycki')

all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))   
    
with open(f'data/mikolajmarszycki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/mikolajmarszycki_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
    
    
    
#%%Uploading files on Google Drive  
    
gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/mikolajmarszycki_{datetime.today().date()}.xlsx", f'data/mikolajmarszycki_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
    
    
    
    