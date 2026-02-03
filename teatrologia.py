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
    blacklist = {'https://teatrologia.pl',}
    full_links = []
    for url in sitemap_urls:
        html_text_sitemap = requests.get(url).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        links = [e.text for e in soup.find_all('loc') if e.text not in blacklist]
        full_links.extend(links)
    full_links = [(e.split('/')[3], e) for e in full_links]
    return full_links

def dictionary_of_article(article_tuple):
    
    section, article_link = article_tuple
    time.sleep(0.5)
    
    response = requests.get(article_link)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
           
    # author and title
    authors_title = soup.find('title').text.replace(' | Teatrologia info', '')
    authors_title = authors_title.split('/')
    if len(authors_title) > 1:
        author = authors_title[0].strip()
        title = authors_title[1].strip()
    else:
        author = ''
        title = authors_title[0].strip()
    
    # date
    date = soup.find('time', class_='entry-date published').get('datetime')
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
    date_of_publication = date.strftime('%Y-%m-%d')
           
    if art_content := soup.find('div', class_='entry-content'):
        text_of_article = art_content
        article = text_of_article.text.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None
            
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
                             'Sekcja': section,
                             'Numer': issues_links.get(article_link, None),
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    dictionary_of_article.update(related_dict)
    all_results.append(dictionary_of_article)

def get_issues():
    url = 'https://teatrologia.pl/category/wydania/'
    response = requests.get(url)
    if response.status_code != 200:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    issues = soup.find('ul', class_='menu').find_all('li')
    issues = [(e.find('a').get('href'), e.text) for e in issues]
    
    issue_links = dict()
    for elem in tqdm(issues):
        response_art = requests.get(elem[0])
        html_text = response_art.text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.find('a').get('href') for e in soup.find_all('article')]
        for link in links:
           issue_links[link] = elem[1] 
   
    return issue_links
    

#%%
sitemap_urls = [
    'https://teatrologia.pl/sitemaps/post-sitemap1.xml',
    'https://teatrologia.pl/sitemaps/post-sitemap2.xml',
    ]
articles_links = process_sitemap(sitemap_urls)

issues_links = get_issues()

all_results = []
with ThreadPoolExecutor(max_workers=5) as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


with open(f'data/teatrologia_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)
   
with pd.ExcelWriter(f"data/teatrologia_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)    

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/teatrologia_{datetime.today().date()}.xlsx", f'data/teatrologia_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  