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


#%%def
def articles_web_scraping():
    pages = []
    for e in range(0,8):
        pages.append(f'https://www.salon24.pl/u/wspomnienia/posts/0,{e},wszystkie')
    pages_links = []
    for page in tqdm(pages):
        # page = pages[0]
        html_text_sitemap = requests.get(page).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        links = [e.find('a')['href'] for e in soup.find_all('h3', {'class': 'title'})]
        pages_links.extend(links)
    return pages_links  

def dictionary_of_article(article_link):
    # article_link = sitemap_links[1]
    # article_link = 'https://pismointer.wordpress.com/aktualny-numer/krytyka/rafal-rozewicz-co-pozostaje-czytajac-to-bruk-piotra-gajdy-i-trzeci-migdal-mirki-szychowiak/'
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('meta', {'property': 'article:published_time'})['content'][:10]
    
    content_of_article = soup.find('div', class_='post-entry')
    
    author = soup.find('h2', {'style': 'text-align:center;'})
    if author:
        author = author.text.strip()     
    else:
        author = None
        
    author_bio = soup.find_all('p', {'style': 'text-align:justify;'})
    if author_bio:
        author_bio = author_bio[-1].text.strip()
    else:
        author_bio = None
    
    title_of_article = soup.find('h1', class_='post-title')
 
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None
    
    tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')])
    
    text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
    
    try:
        external_links = ' | '.join(set([e['href'] for e in content_of_article.find_all('div') if 'href' in e.attrs and 'pismointer' not in e['href']]))
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
        photos_links = None
        
    dictionary_of_article = {'Link' : article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Autor bio': author_bio,
                             'Tagi': tags if tags != '' else None,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links} 
     
    all_results.append(dictionary_of_article)    



#%%main

article_links = articles_web_scraping()

all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, sitemap_links),total=len(sitemap_links)))   
    
with open(f'data/pismointer_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/pismointer_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
    
    
    
#%%Uploading files on Google Drive  
    
gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/pismointer_{datetime.today().date()}.xlsx", f'data/pismointer_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
    
    
    
    