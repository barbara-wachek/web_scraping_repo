#%% import 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% def    
def get_articles_links(link):
    #link = 'https://okiem-widza.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'xml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    return sitemap_links
    
    
def dictionary_of_article(article_link):
    # article_link = articles_links[0]
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    try:
        date = soup.find('h2', class_='date-header').find('span').text
        date_of_publication = date_change_format_long(date)
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h2', class_='post-title entry-title').text.strip().replace('\xa0', ' ')
    except:
        title_of_article = None
    
    try:
        author = 'Ewa Bąk'
        # author = soup.find('a', class_='g-profile').find('span').text 
    except:
        author = None
    
        
    article = soup.find('div', class_='post-body entry-content')
    
    try:
        text_of_article = article.text.replace("  ", " ").replace('\n', '')
    except:
        text_of_article = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'okiem-widza', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None 
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
    
    all_results.append(dictionary_of_article)


#%% main
articles_links = get_articles_links('https://okiem-widza.blogspot.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/okiem-widza_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/okiem-widza_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/okiem-widza_{datetime.today().date()}.xlsx", f'data/okiem-widza_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   