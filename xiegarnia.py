#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from functions import date_change_format_short


#%% def
def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'https://xiegarnia.pl/sitemap_index.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if '/post-' in x.text]
    art_links = []
    for link in tqdm(links):
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, "html.parser")
        site_links = [e.text for e in soup.find_all('loc')]
        art_links.extend(site_links)  
    return art_links

# kategorie = set([e.split('/')[3] for e in articles_links if e.split('/')[3]])

# examples = []
# for el in kategorie:
#     for e in articles_links:
#         if f"/{el}/" in e:
#             examples.append(e)
#             break


def dictionary_of_article(link):
    # link = examples[9]
    # link = 'https://xiegarnia.pl/recenzje/wodne-szalenstwo/'

    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = date_change_format_short(soup.find('div', class_='date left').text)
   
    content_of_article = soup.find('section', class_='section-article').find('article')
    
    tags = link.split('/')[3]
    
    author = soup.find('a', rel='author')
    if author:
        author = author.text
    else:
        author = None
    
    text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article.find_all('p')])
    
    title_of_article = soup.find('h2', class_='article-header')
 
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'xiegarnia' not in el['href']]))
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])
    except KeyError:
        photos_links = None
    
    if tags == 'recenzje':
        try:
            book_title = soup.find('h4', class_='aside-header').text
            book_author = soup.find_all('dd')[0].text.strip()
            book_publisher = soup.find_all('dd')[1].text.strip()
            book_date = soup.find_all('dd')[2].text.strip()
        except AttributeError:
            book_title = None
            book_author = None
            book_publisher = None
            book_date = None
    else:
        book_title = None
        book_author = None
        book_publisher = None
        book_date = None

    dictionary_of_article = {'Link': link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tagi': tags,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Autor książki': book_author,
                             'Tytuł książki': book_title,
                             'Wydawnictwo': book_publisher,
                             'Rok i miejsce wydania': book_date}

    all_results.append(dictionary_of_article)
    
#%% main
articles_links = get_links_of_sitemap('https://xiegarnia.pl/sitemap_index.xml')
articles_links = [e for e in articles_links if not any(el in e for el in ['/konkursy/', '/polecane/', '/media-o-nas/']) and e != 'https://xiegarnia.pl/']

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/xiegarnia_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/xiegarnia_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/xiegarnia_{datetime.today().date()}.xlsx", f'data/xiegarnia_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















