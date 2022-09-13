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
from functions import date_change_format_short
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%def
def krytycznym_okiem_web_scraping_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)
    
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'lxml')
    
    author = "Jarosław Czechowicz"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    date_of_publication = soup.find('h2', class_='date-header').text
    text_of_article = soup.find('div', class_='post-body entry-content')
    article = text_of_article.text.strip().replace('\xa0', ' ')
    tags_span = soup.find_all('span', class_='post-labels')     
    tags = [tag.text.replace('Etykiety:', '').strip().replace('\n', ' ') for tag in tags_span][0]
    
    try:
        author_of_book = re.findall(r'(?<=[\"|\”]\s)(.*\s*\.*\s*?\.*?)', title_of_article)[0] 
    except IndexError:
        author_of_book = None    
        
    try:
        title_of_book = re.findall(r'[\"\„].*[\"\”](?=\s.*)', title_of_article)[0]
    except IndexError:
        title_of_book = None
        
    try:
        title_of_review = re.findall(r'(?<=Tytuł recenzji:\s).*\s?.*?(?=\p{Lu}|\n)', article)[0].replace('\n', ' ').strip() 
        
    except (AttributeError, KeyError, IndexError):
        title_of_review = None

    try:
        publication_of_book = date_change_format_short(re.findall(r'(?<=Data wydania:\s).*(?=\n)', article)[0])
        year_of_publication = re.sub(r'(\d{4})(\-\d{2})(\-\d{2})', r'\1', publication_of_book)
    except (AttributeError, KeyError, IndexError, ValueError):
        year_of_publication = None
        
    try:
        publisher = re.findall(r'(?<=Wydawca:\s).*\s?.*?(?=\n|D)', article)[0].replace('\n', ' ').strip()
    except (AttributeError, KeyError, IndexError):
        publisher = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|krytycznymokiem', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
         
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': article.replace('\n', ' '),
                             'Odautorski tytuł recenzji': title_of_review if type(title_of_review) == str and len(title_of_review) < 60 else None,
                             'Rok wydania': year_of_publication,
                             'Wydawnictwo': publisher if type(publisher) == str and len(publisher) < 60 else None, 
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }
            
    all_results.append(dictionary_of_article)
        

#%% main
sitemap_links = krytycznym_okiem_web_scraping_sitemap('http://krytycznymokiem.blogspot.com/sitemap.xml')    
articles_links = []    

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'krytycznym_okiem_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)   


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
      
with pd.ExcelWriter(f"krytycznym_okiem_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"krytycznym_okiem_{datetime.today().date()}.xlsx", f'krytycznym_okiem_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()         














