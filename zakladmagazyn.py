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
from functions import date_change_format_long


#%% def    

def get_article_links(sitemap_link):

    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    # article_link = 'https://dom-echa.blogspot.com/2022/12/aneksja-kulturowa-ani-z-zielonego.html'
    # article_link = 'https://dom-echa.blogspot.com/2021/07/janka-i-janek-louisa-may-alcott.html'
    # article_link = 'https://dom-echa.blogspot.com/2020/10/w-cieniu-bzow-louisa-may-alcott.html'
    # article_link = 'https://dom-echa.blogspot.com/2017/06/janina-buchholtzowa-czesc-dalsza-2.html'
   
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        author = soup.find('a', class_='g-profile').span.text
    except:
        author = None
    
    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    except: 
        title_of_article = None
        
    try:
        date_of_publication = soup.find('h2', class_='date-header').span.text
        date_of_publication = date_change_format_long(date_of_publication)
    except:
        date_of_publication = None
    

    article = soup.find('div', class_='post-body entry-content')
    
    try:
        text_of_article = " ".join([
            x.get_text(separator=" ").strip().replace('\xa0', ' ').replace('\n', ' ').replace("  ", ' ')
            for x in article.find_all(lambda tag: (
                tag.name == 'p' or (tag.name == 'div' and 'MsoNormal' in tag.get('class', []))
            ))
        ])
    except:
        text_of_article = None
    

    try:
        author_of_book = re.search(r"/\s*(.+)$", title_of_article).group(1)
    except:
        author_of_book = None
         
    try:
        title_of_book = re.search(r"^(.*?)\s*/", title_of_article).group(1)
    except:
        title_of_book = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|dom-echa', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('')
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/dom-echa_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/dom-echa_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    