#%% import
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import mktime
import json
from functions import date_change_format_long
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%% def
def web_scraping_sitemap(sitemap):
    # sitemap = 'https://teatruglodna.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links   
    
def dictionary_of_article(article_link):
    #article_link = articles_links[0]
    driver.get(article_link)
    time.sleep(3)
    html_text = driver.page_source
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('abbr', class_='time published')['title'][:10]
   
    content_of_article = soup.find('div', class_='article-content entry-content')
    
    tags = '|'.join([e.text for e in soup.find_all('a', {'class': 'label'})])
  
    try:
        author = soup.find('a', {'itemprop': 'author'}).text 
    except:
        author = None
        
    text_of_article = content_of_article.text.strip().replace('\n', '')
 
    title_of_article = soup.find('h1', class_='title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(blogspot)|(jpg)', x)])
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
        photos_links = None

    dictionary_of_article = {'Link': article_link,
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

articles_links = web_scraping_sitemap('https://teatruglodna.blogspot.com/sitemap.xml')

driver = webdriver.Firefox()
    
all_results = []
for article_link in tqdm(articles_links):
    dictionary_of_article(article_link)

# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/teatruglodna_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   
    
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)
    
with pd.ExcelWriter(f"data/teatruglodna_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posty', index=False)    
    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/teatruglodna_{datetime.today().date()}.xlsx", f'data/teatruglodna_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    
    
    
    
    
    
    
    
    
    
        
        