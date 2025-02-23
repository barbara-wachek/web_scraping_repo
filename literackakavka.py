
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
from functions import date_change_format_short, get_links
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive


#%% def     
    
def dictionary_of_article(article_link):
    # article_link = 'https://literackakavka.pl/bezmiar-miar/'
    # article_link = 'https://literackakavka.pl/guzikologia/'
    # article_link = 'https://literackakavka.pl/i-stala-sie-ciemnosc/'
    # article_link = 'https://literackakavka.pl/bezmiar-miar/'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    author = 'Georgina Gryboś'

    try:
        title_of_article = soup.find('h1', class_='elementor-heading-title elementor-size-default').text.strip()
    except:
        title_of_article = None
        
    try:    
        date = soup.find('time').text
        date_of_publication = re.sub(r'(\d{1,2})(\s\p{L}*)\,(\s\d{4})', r'\1\2\3', date)
        date_of_publication = date_change_format_short(date_of_publication)

    except: 
        date_of_publication = None
        
    
    article = soup.find_all('div', class_='elementor-widget-container')[-3]
    
    ''' Kategoria wpisu ''' 
    
    post_div = soup.find('div', class_='elementor-location-single')
   
    if post_div:
        classes = post_div.get('class', [])
        category = next((cls.replace('category-', '') for cls in classes if cls.startswith('category-')), None)
       
    else:
        category = None
    
     
    try:
        title_of_piece = re.search(r"„[^„”]+?”", title_of_article).group(0)
    except (AttributeError, TypeError):
        title_of_piece = None
        
    
    try:
        text_of_article = " ".join([x.text for x in soup.find_all('p')])
    except:
        text_of_article = None
            
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'literackakavka', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tytuł dzieła': title_of_piece,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)



#%% main
articles_links = get_links('https://literackakavka.pl/post-sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/literackakavka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data/literackakavka_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"data/dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
# 	gfile.SetContentFile(upload_file)
# 	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   