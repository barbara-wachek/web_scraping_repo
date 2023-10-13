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
    response = requests.get(article_link)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    if not soup.find('article'):
        return
        
    if meta_description := soup.find('meta', {'name': 'description'}):
        meta_description = soup.find('meta', {'name': 'description'})['content']
    else: meta_description = None
    
    if art_header := soup.find('div', class_='articles-header-content'):
        date_of_publication = art_header.find('span').text.split(',')[0]
        date_of_publication = datetime.strptime(date_of_publication, '%d.%m.%Y')
        date_of_publication = date_of_publication.strftime('%Y-%m-%d')
        title_of_article = art_header.find('h1').text
    else: 
        date_of_publication, title_of_article = None, None
    
    if art_content := soup.find('div', class_='article-content'):
        text_of_article = art_content
        article = text_of_article.text.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None
        
    if art_info := soup.find('div', class_='article-info'):
        art_info_dict = {}
        for column in art_info.find_all('div', class_='column'):
            keys = [e.text.replace(':', '').strip() for e in column.find_all('h6')]
            values = [e.text.replace('Link do źródła', '').strip() for e in column.find_all('p')]
            art_info_dict.update(dict(zip(keys, values)))
    else: 
        art_info_dict = {}
    
    try:
        if related := soup.find('section', class_='box'):
            related_dict = {}
            for column in related.find_all('div', class_='column'):
                key = column.find('h6').text
                values = ' | '.join([e.text.strip() for e in column.find_all('li')])
                related_dict[key] = values
        else:
            related_dict = {}  
    except (AttributeError, KeyError, IndexError): 
        related_dict = {}    
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|intimathule', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             'Opis': meta_description,
                             }
    dictionary_of_article.update(art_info_dict)
    dictionary_of_article.update(related_dict)
    all_results.append(dictionary_of_article)

#%%
articles_links = []
for idx in range(1,5):
    sitemap_url = f'https://www.e-teatr.pl/sitemap_{idx}.xml'
    sitemap_links = process_sitemap(sitemap_url)
    articles_links.extend(sitemap_links)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/eteatr_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/eteatr_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/eteatr_{datetime.today().date()}.xlsx", f'data/eteatr_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  