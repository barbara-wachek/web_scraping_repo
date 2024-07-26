#%%import
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
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def
def process_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text[9:-3] for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    response = requests.get(article_link)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    if not soup.find('article'):
        return

    # title
    if title_of_article := soup.find('h2', class_='entry-title'):
        title_of_article = title_of_article.text
    else:
        title_of_article = None

    # author
    author = None
        
    # date
    if date_div := soup.find('li', class_='meta-date'):
        months_map = {
                'stycznia': '01',
                'lutego': '02',
                'marca': '03',
                'kwietnia': '04',
                'maja': '05',
                'czerwca': '06',
                'lipca': '07',
                'sierpnia': '08',
                'września': '09',
                'października': '10',
                'listopada': '11',
                'grudnia': '12'
                }
        day, month, year = date_div.text.replace('Post published:', '').split()
        month = months_map[month]
        date = datetime.strptime(f'{day} {month} {year}', '%d %m %Y')
        date = date.strftime('%Y-%m-%d')
    else:
        date = None
        
    # category
    if cat_div := soup.find('li', class_='meta-cat'):
        category = [e.text for e in cat_div.find_all('a')]
        category = ' | '.join(category)
    else:
        category = None
    
    # content
    if art_content := soup.find('div', class_='entry-content'):
        text_of_article = ' '.join([p.text for p in art_content.find_all('p')])
        article = text_of_article.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
articles_links = []

sitemap_links = [
        'https://qlturka.pl/post-sitemap.xml',
        'https://qlturka.pl/post-sitemap2.xml',
        'https://qlturka.pl/post-sitemap3.xml',
        'https://qlturka.pl/post-sitemap4.xml',
        'https://qlturka.pl/post-sitemap5.xml',
        'https://qlturka.pl/post-sitemap6.xml',
    ]

for sitemap_url in tqdm(sitemap_links):
    sitemap_links = process_sitemap(sitemap_url)
    articles_links.extend(sitemap_links)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/qlturka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/qlturka_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/qlturka_{datetime.today().date()}.xlsx", f'data/qlturka_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  