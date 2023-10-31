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
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%% def

def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'https://www.musisieukazac.pl/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if re.findall(r'^https://www.musisieukazac.pl/\d+', x.text)]
    links = [e for e in links if 'wstep' in e]
    return links


def dictionary_of_issues(link):
    # link = articles_links[0]
    # link = articles_links[1]
    # link = articles_links[2]
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    date_of_publication = soup.find('time', class_='entry-date published')['datetime']
    date_of_publication = datetime.fromisoformat(date_of_publication).date()
    
    text_of_article = soup.find('div', class_='entry-content')
    article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    
    title_of_article = soup.find('h1', class_='entry-title').text
    
    issue_articles_list = soup.select('#main li')
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'musisieukazac', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')][1:])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    dictionary_of_article = {"Link": link, 
                             "Data publikacji": date_of_publication,
                             "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                             "Tekst artykułu": article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }
    
    all_results.append(dictionary_of_article)
    total_articles_list.extend(issue_articles_list)
    
def dictionary_of_articles(art_tag):
# for art_tag in tqdm(total_articles_list):
    # art_tag = total_articles_list[0]
    # art_tag = total_articles_list[1]
    # art_tag = total_articles_list[2]

    try:
        link = art_tag.find('a')['href']
        author = art_tag.text.split(' – ')[0]
        try:
            graphic = re.findall('grafika\:.*?(?=,|$)', art_tag.text)[0]
        except IndexError: graphic = None
        try:
            editor = re.findall('redakcja tekstu\:.*?(?=,|$)', art_tag.text)[0]
        except IndexError: editor = None
        
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')

        try:
            date_of_publication = soup.find('time', class_='entry-date published')['datetime']
        except TypeError:
            date_of_publication = soup.find('time', class_='entry-date published updated')['datetime']
        date_of_publication = datetime.fromisoformat(date_of_publication).date()
        
        text_of_article = soup.find('div', class_='entry-content')
        article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
        
        title_of_article = soup.find('h1', class_='entry-title').text
          
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'musisieukazac', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        try: 
            photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')][1:])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None
        
        dictionary_of_article = {"Link": link, 
                                 'Autor': author,
                                 'Grafika': graphic,
                                 'Redakcja': editor,
                                 "Data publikacji": date_of_publication,
                                 "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                                 "Tekst artykułu": article,
                                 'Linki zewnętrzne': external_links,
                                 'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img') if 'src' in x] else False,
                                 'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                                 'Linki do zdjęć': photos_links
                                 }
        
        all_results.append(dictionary_of_article)
    except (KeyError, TypeError):
        pass
    

#%% main
articles_links = get_links_of_sitemap('https://www.musisieukazac.pl/sitemap.xml')

all_results = []
total_articles_list = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_issues, articles_links),total=len(articles_links)))

total_articles_list = [e for e in total_articles_list if e.find('a')]

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_articles, total_articles_list),total=len(total_articles_list)))
    

with open(f'data/musisieukazac_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        


df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)


with pd.ExcelWriter(f"data/musisieukazac_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/musisieukazac_{datetime.today().date()}.xlsx", f'data/musisieukazac_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















