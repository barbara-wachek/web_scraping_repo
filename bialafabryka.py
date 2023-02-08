#%% import
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% def

def biala_fabryka_web_scraping_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links   

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

    date_of_publication = soup.find('h2', class_='date-header').text
    new_date = date_change_format_long(date_of_publication)
    text_of_article = soup.find('div', class_='post-body entry-content')
    article = text_of_article.text.strip().replace('\n', ' ')
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    tags_span = soup.find_all('span', class_='post-labels')
    tags = ' | '.join([tag.text.replace(',','').replace('Etykiety:', '') for tag in tags_span][0].strip().split('\n'))
    
    try:
        work_description = [x.text for x in text_of_article.find_all('h2')][0].strip().replace('\n', ' ')
    except (AttributeError, KeyError, IndexError, TypeError):
        work_description = None
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
                             'Autor': 'Piotr Gajda' if re.findall(r'(Autor\:\sPiotr Gajda)|(\(pg\))', article) else "Krzysztof Kleszcz",
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Opis książki/płyty': work_description,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
                             

    all_results.append(dictionary_of_article)
        
    
#%% main

sitemap_links = biala_fabryka_web_scraping_sitemap('https://bialafabryka.blogspot.com/sitemap.xml')

articles_links = []  

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
    
all_results = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    
with open(f'biała_fabryka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f,ensure_ascii=False)        
           

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"bialafabryka_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"bialafabryka_{datetime.today().date()}.xlsx", f'biała_fabryka_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  














    
    
    
    
    
    
