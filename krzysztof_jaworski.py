#%% import 
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
from functions import date_change_format_long, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def

def get_article_pages(link):   
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)   
    

def dictionary_of_article(article_link):
    
    # article_link = 'http://krzysztofjaworski.blogspot.com/2021/12/przewodnik-po-zaminowanym-terenie-2.html'
    # article_link = 'https://krzysztofjaworski.blogspot.com/2013/07/andrzej-lenartowski-listy-z-grobow.html'
    #article_link = 'https://krzysztofjaworski.blogspot.com/2014/07/program-merkury.html'
    
    
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    author = soup.find('a', class_='g-profile')
    if author:
        author = author.span.text
    else:
        author = None
    
    
    
    title_of_article = soup.find('h3', class_='post-title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()
    else:
        title_of_article = None


    date_of_publication = soup.find('h2', class_='date-header')
    if date_of_publication:
        new_date = date_change_format_long(date_of_publication.text)
    else:
        new_date = None
    
    
    
    content_of_article = soup.find('div', class_='post-body entry-content')
    
    if content_of_article:
        text_of_article = content_of_article.text
        if text_of_article:
            text_of_article = content_of_article.text.strip().replace('\n', '').replace('\xa0', '')
        else:
            text_of_article = None
    else:
        text_of_article = None
        
    
   
    tags = soup.find('span', class_='post-labels')
    if tags:
        tags = " | ".join([x.text for x in soup.find('span', class_='post-labels').findChildren('a')])
    else:
        tags = None

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|krzysztofjaworski', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)




#%%main
sitemap_links = get_links('https://krzysztofjaworski.blogspot.com/sitemap.xml')

articles_links = []    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


with open(f'krzysztof_jaworski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)    

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"krzysztof_jaworski_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"krzysztof_jaworski__{datetime.today().date()}.xlsx", f'krzysztof_jaworski__{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  















    
    