#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_short, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def
def get_sitemap_links(link):
    # link = "https://kempinsky.pl/sitemap_index.xml"
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_pages = [e.text for e in soup.find_all('loc') if 'post-sitemap' in e.text]
    return links_pages 

def get_links_from_sitemap(link):
    # link = sitemap_links[0]
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_pages = [e.text for e in soup.find_all('loc')]
    articles_links.extend(links_pages)
 
#%%main     
 
sitemap_links = get_sitemap_links('https://kempinsky.pl/sitemap_index.xml')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_from_sitemap, sitemap_links),total=len(sitemap_links)))

month_dict = {'sty': "January",
              'lut': "February",
              'mar': "March",
              'kwi': "April",
              'maj': "May",
              'cze': "June",
              'lip': "July",
              'sie': "August",
              'wrz': "September",
              'paź': "October",
              'lis': "November",
              'gru': "December"}

all_results = []
errors = []
# def dictionary_of_article(article_link): --> przy wielowątkowości błędy
for article_link in tqdm(articles_links[1503:]):
    # article_link = articles_links[18]
    # article_link = 'https://kempinsky.pl/odrobina-madrosci-w-lodzi/'
    
    try:
        r = requests.get(article_link)
        html_text = r.text
        while '429 Too Many Requests' in html_text:
            time.sleep(5)
            html_text = requests.get(article_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        date_of_publication = re.findall('(?>on)(.+?)(?=in)', soup.find('p', class_= 'post-meta').text.strip())[0].strip()
        date_of_publication = date_of_publication.replace(date_of_publication[:3], month_dict.get(date_of_publication[:3]))
        date_of_publication = datetime.strptime(date_of_publication, '%B %d, %Y').date()
        
        category = soup.find('a', {'rel': 'category tag'}).text
        text_of_article = soup.find('div', class_='post-text')
        article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
        author = 'Grzegorz Kempinsky'
        title_of_article = soup.find('h1', class_='title').text
        tags = '|'.join(set([e.text for e in soup.find_all('a', {'rel': 'tag'})]))
        
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'kempinsky', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        try: 
            photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])
        except (AttributeError, KeyError, IndexError):
            photos_links = None
        
        dictionary_of_article = {"Link": article_link, 
                                 "Data publikacji": date_of_publication,
                                 "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                                 "Tekst artykułu": article,
                                 "Autor": author,
                                 "Tagi": tags,
                                 "Kategoria": category,
                                 'Linki zewnętrzne': external_links,
                                 'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                                 'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                                 'Linki do zdjęć': photos_links
                                 }
        
        all_results.append(dictionary_of_article)
    except:
        errors.append(article_link)

# all_results = [] 
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links))) 
    
with open(f'data/kempinsky_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)           

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/kempinsky_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)  


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/kempinsky_{datetime.today().date()}.xlsx", f'data/kempinsky_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  










   
    
    
    
    
    
    