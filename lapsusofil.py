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
from functions import date_change_format_long, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% def    
def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'xml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)   
    
def dictionary_of_article(article_link):
    # article_link = 'https://lapsusofil.blogspot.com/2025/02/o-filmie-dziewczyna-z-iga.html'
    # article_link = 'https://lapsusofil.blogspot.com/2014/09/recenzja-ksiazki-joanny-dziwak-gry.html'
    # article_link = 'https://lapsusofil.blogspot.com/2012/11/recenzja-ksiazki-oriany-fallaci.html'
    # article_link = 'https://lapsusofil.blogspot.com/2022/11/refleksja-o-ksiazce-gaby-krzyzanowskiej.html'
    # article_link = 'https://lapsusofil.blogspot.com/2012/08/recenzja-ksiazki-czycz-i-filmowcy.html'
    # article_link = 'https://lapsusofil.blogspot.com/2011/10/sandor-marai-pokrzepiciel.html'
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        author = soup.find('span', {'itemprop':'name'}).text
    except AttributeError:
        author = None
    
    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    except:
        title_of_article = None
        
        
    date = soup.find('h2', class_='date-header').text
    date_of_publication = date_change_format_long(date)
    article = soup.find('div', class_='post-body entry-content')
    
    try:
        text_of_article = article.text.strip().replace("\n", " ").replace("  ", " ")
    except:
        text_of_article = None
    
    
    try:
        title_of_piece = re.search(r'\".*\"', title_of_article).group(0)
    except: 
        title_of_piece= None

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|lapsusofil', x)])
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
                             'Tytuł dzieła': title_of_piece,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
articles_links = get_links('https://lapsusofil.blogspot.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/lapsusofil_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data/lapsusofil_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   