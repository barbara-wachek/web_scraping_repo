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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%% def

def dyskretny_urok_drobiazgow_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links   
    
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('abbr')['title']
    new_date = re.sub(r'([\d-]*)(T.*)', r'\1', date_of_publication)
    text_of_article = soup.find('div', class_='post-body')
    article = text_of_article.text.strip().replace('\n', ' ')
    tags_span = soup.find_all('span', class_='label-info')
    title_of_article = soup.find('h3', class_='post-title').text.strip()
    
    try:
        tags = [x.text.replace('\n', '').replace('  ', "").replace(',', ' | ') for x in tags_span][0]
    except(IndexError):
        tags = None
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|dyskretny', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': new_date,
                             'Autor': 'Kinga Piotrowiak-Junkiert',
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    

#%% main

articles_links = dyskretny_urok_drobiazgow_sitemap('http://dyskretnyurokdrobiazgow.blogspot.com/sitemap.xml')    

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'dyskretny_urok_drobiazgow_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"dyskretny_urok_drobiazgow_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"dyskretny_urok_drobiazgow_{datetime.today().date()}.xlsx", f'dyskretny_urok_drobiazgow_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  










