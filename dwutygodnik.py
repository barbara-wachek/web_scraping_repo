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
    if not response.ok:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    categories = ['cat--literatura', 'cat--film', 'cat--teatr', 'cat--felietony']
    for cat in categories:
        if soup.find('a', class_=cat):
            break
    else: return
     
    # title + section
    title_of_article, section = [e.strip() for e in soup.title.text.split('|')[:2]]
    
    # description
    description = soup.find('meta', {'name': 'description'})['content']
    
    
    if section == 'felieton':
        # author
        if author := soup.find_all('h2', class_='article-aside-feuilleton__author'):
            author = list(set([e.text.strip() for e in author]))
            author = ' | '. join(author)
        else:
            author = None
        
        # date
        date_of_publication = None    
        
        # issue
        issue = None    
    
        # tags
        if tags := soup.find('div', class_='article-property__value--tags'):
            tags = [e.text.strip() for e in tags.find_all('a')]
            tags = ' | '.join(tags)
        else:
            tags = None
    
        # content
        if art_content := soup.find('div', class_='article-content'):
            text_of_article = art_content
            article = text_of_article.text.strip().replace('\n', ' ')
        else:
            text_of_article, article = None, None
             
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a') if x['href']]])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        try: 
            photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img') if x['src']])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None
    else:    
        # author
        if author := soup.find_all('a', class_='article-property__author-link'):
            author = list(set([e.text.strip() for e in author]))
            author = ' | '. join(author)
        else:
            author = None
        
        # date
        if date_of_publication := soup.find('div', class_='article-property__value--date'):
            date_of_publication = date_of_publication.text
        else:
            date_of_publication = None    
        
        # issue
        if issue := soup.find('div', class_='article-property__value--issue'):
            issue = issue.text
        else:
            issue = None    
    
        # tags
        if tags := soup.find('div', class_='article-property__value--tags'):
            tags = [e.text.strip() for e in tags.find_all('a')]
            tags = ' | '.join(tags)
        else:
            tags = None
    
        # content
        if art_content := soup.find('div', class_='article-content'):
            text_of_article = art_content
            article = text_of_article.text.strip().replace('\n', ' ')
        else:
            text_of_article, article = None, None
             
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a') if x['href']]])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        try: 
            photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img') if x['src']])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None
            

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Sekcja': section,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Opis': description,
                             'Numer': issue,
                             'Tagi': tags,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
articles_links = []
sitemap_url = 'https://www.dwutygodnik.com/sitemap/'
sitemap_links = process_sitemap(sitemap_url)
articles_links = [link for link in sitemap_links if '/artykul/' in link]

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    

with open(f'data/dwutygodnik_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/dwutygodnik_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/dwutygodnik_{datetime.today().date()}.xlsx", f'data/dwutygodnik_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  