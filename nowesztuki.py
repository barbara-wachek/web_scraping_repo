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
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    article_link
    
    response = requests.get(article_link)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    # title
    if title_of_article := soup.find('div', class_='contents').find('h1'):
        title_of_article = title_of_article.text
    else:
        title_of_article = None

    # author
    if content_paragraphs := soup.find('div', class_='contents').find_all('p'):       
        first_paragraph = content_paragraphs[0].text
        if first_paragraph.startswith('autor:') or first_paragraph.startswith('autorka:'):
            author = first_paragraph.split(':')[1].strip()
        else: author = None
    else:
        author = None

    # date and category
    if article_content := soup.find('div', class_='contents'):
        header = article_content.find_previous_sibling('div')
        category = header.find_all('p')[0].text.strip()
        date = header.find_all('p')[1].text.strip()
        date = datetime.strptime(date, '%d.%m.%Y')
        date = date.strftime('%Y-%m-%d')
    else:
        category, date = None, None
    
    # content
    if art_content := soup.find('div', class_='contents'):
        text_of_article = ' '.join([p.text for p in art_content.find_all('p')])
        article = text_of_article.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
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
sitemap_url = f'https://nowesztuki.pl/post-sitemap.xml'
sitemap_links = process_sitemap(sitemap_url)
articles_links.extend(sitemap_links)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/nowesztuki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/nowesztuki_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/nowesztuki_{datetime.today().date()}.xlsx", f'data/nowesztuki_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
