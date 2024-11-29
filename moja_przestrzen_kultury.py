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
from functions import get_links


#%% def    

def dictionary_of_article(article_link):
    article_link = 'https://mojaprzestrzenkultury.pl/archiwa/1722'
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date_of_publication = soup.find('time', class_='entry-date published')
    
    # try:
    #     author = soup.find('span', {'itemprop':'name'}).text
    # except AttributeError:
    #     author = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
     
    try:
        category = soup.find('div', class_='entry-categories').text.strip()
    except:
        category = None
    
        
    try:
        tags = [x.text for x in soup.find('div', class_='entry-tags clearfix').find_all('a')]
    except:
        tags = None
        
    
    
    article = soup.find('div', class_='post-body entry-content')
    
    try:
        text_of_article = article.text.strip().replace("\n", " ").replace("  ", " ")
    except:
        text_of_article = None
    
    
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='post-labels').find_all('a')]) 
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|dakowicz', x)])
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
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
articles_links = get_links('https://mojaprzestrzenkultury.pl/post-sitemap.xml')

# articles_links_yoast = get_links('https://mojaprzestrzenkultury.pl/post-sitemap.xml')  #Są dwie sitemapy, więc porownałam ilosc linków do artykułów. Obie mają tyle samo linków. 


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data\\dakowicz_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data\\dakowicz_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data\\dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   