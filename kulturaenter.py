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
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive


#%% def    
def get_articles_links(link):   
    # link = 'https://kulturaenter.pl/wp-sitemap-posts-article-1.xml'
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'xml')
    sitemap_links = [e.text for e in soup.find_all('loc') if re.search(r'https\:\/\/kulturaenter\.pl\/.+', e.text)]
    return sitemap_links
    
    
    
def dictionary_of_article(article_link):
    # article_link = 'https://kulturaenter.pl/article/ola-test/'
    # article_link = 'https://kulturaenter.pl/article/galeria-lubelskie-sciany-%e2%88%92-interakcje/' (tylko zdjęcia)
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    
    try:
        title_of_article = soup.find('h1', class_='title-heading-left').text.strip().replace('\xa0', ' ')
    except:
        title_of_article = None
    
    author = None   
    
    try:
        spans = [{x.find('a').text.strip().replace('\xa0', ' ') : x.find('span').text.strip().replace('\xa0', ' ') for x in soup.find_all('p') if x.find('a') and x.find('span')}]
        for x,y in spans[0].items():
            if x == title_of_article:
                author = y
                break
            else:
                author = None
    except:
        author = None
    
    try:  
        issue = soup.find('div', class_='textwidget custom-html-widget').find('h1').text.replace('\xa0', " ")
    except:
        issue = None 
        
    article = soup.find('div', class_='fusion-content-tb fusion-content-tb-1 zawijanie_wierszy')
    
    try:
        text_of_article = "".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    
 
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'kulturaenter', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Numer i rok': issue,
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
articles_links = get_articles_links('https://kulturaenter.pl/wp-sitemap-posts-article-1.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/kulturaenter_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
   
with pd.ExcelWriter(f"data/kulturaenter_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"data/dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
# 	gfile.SetContentFile(upload_file)
# 	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   