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
def process_sitemap(sitemap_urls):
    blacklist = {'https://www.literaturasautee.pl/',}
    full_links = []
    for url in sitemap_urls:
        html_text_sitemap = requests.get(url).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        links = [e.text for e in soup.find_all('loc') if e.text not in blacklist]
        full_links.extend(links)
    return full_links

def dictionary_of_article(article_link):
    time.sleep(0.5)
    
    response = requests.get(article_link)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
         
    # title
    title = soup.find('h1', class_='entry-title').text.strip()
    
    # author
    if  author := soup.find('span', class_='author'):
        author = author.text.strip()
    else:
        author = None
    
    # date
    date = soup.find('time', class_='entry-date').get('datetime')
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
    date_of_publication = date.strftime('%Y-%m-%d')
    
    # category
    category = soup.find_all('a', rel='category tag')
    category = ' | '.join([e.text.strip() for e in category])
    
           
    if art_content := soup.find('div', class_='entry-content'):
        text_of_article = art_content
        article = text_of_article.text.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None
        
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|intimathule|poledwumiesiecznik', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title,
                             'Kategoria': category,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)
    

#%%
sitemap_urls = [
    'https://www.literaturasautee.pl/post-sitemap.xml',
    ]
articles_links = process_sitemap(sitemap_urls)

all_results = []
with ThreadPoolExecutor(max_workers=5) as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


with open(f'data/literaturasautee_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)
   
with pd.ExcelWriter(f"data/literaturasautee_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)    

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/literaturasautee_{datetime.today().date()}.xlsx", f'data/literaturasautee_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  