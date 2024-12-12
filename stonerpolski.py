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


#%% def

def get_links_of_issues(archive_link):
    # archive_link = 'https://stonerpolski.pl/archiwum/'
    html_text = requests.get(archive_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = ['https://stonerpolski.pl' + e['href'] for e in soup.select('.style-module--link--bfb56')]
    return links

def get_articles_links(issue_link):
    # issue_link = issues_links[0]
    html_text = requests.get(issue_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = ['https://stonerpolski.pl' + e['href'] for e in soup.find_all('a', {'data-testid': 'article-tile'})]
    articles_links.extend(links)
    

def dictionary_of_article(link):
    # link = articles_links[10]
    # link = 'https://stonerpolski.pl/3-wiersze-4/'
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        # date_of_publication = soup.find('abbr', {'class': 'published'})['title'][:10]
       
        content_of_article = soup.find('div', class_='style-module--content--599d6')
        
        tags = '|'.join([e.text for e in soup.find('div', {'class': 'style-module--about--7025c'}).find_all('a') if e.text[0] == '#' or e.text[0] == e.text[0].lower()])
        
        author = '|'.join([e.text for e in soup.find('div', {'class': 'style-module--about--7025c'}).find_all('a') if e.text[0] != '#' and e.text[0] == e.text[0].upper()])
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
        
        title_of_article = soup.find('h1', {'class': 'style-module--title--39cac'})
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'stonerpolski' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link,
                                 # 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except TypeError:
        errors.append(link)
    
#%% main
issues_links = get_links_of_issues('https://stonerpolski.pl/archiwum/')
issues_links.append('https://stonerpolski.pl/numer-9-gonimy-magie/')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, issues_links),total=len(issues_links)))  

all_results = []
# do_over = errors.copy()

errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/stronerpolski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
# df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
# df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/stronerpolski_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/stronerpolski_{datetime.today().date()}.xlsx", f'data/stronerpolski_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















