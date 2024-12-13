#%% import 
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


#%% def    

def get_articles_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    links = [x.text for x in soup.find_all('loc')]
    articles_links.extend(links)
    
    return articles_links


def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    try:
        date_of_publication = soup.find('li', class_='entry-date').text.strip()
        date_of_publication = re.sub(r'(\d\d?\s.*)(\,)(\s\d{4})(.*)', r'\1\3', date_of_publication)
        date_of_publication = date_change_format_short(date_of_publication)
        
    except:
        date_of_publication = None 
        
        
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
        
    try:
        author = soup.find('li', class_="entry-author").find('a').text.strip()
    except:
        author = None


    try:
        tags = " | ".join([x.text for x in soup.find('p', class_='entry-tags').find_all('a')]) 
    except:
        tags = None
        

    article = soup.find('div', class_='text')
    
    try:
       text_of_article = " ".join([e.text for e in article.find_all('p')]).strip()
        
    except:
        text_of_article = None
    
    

    try: 
        about_author = soup.find('p', class_='entry-author-description')
    except: 
        about_author = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'impresjee', x)])
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
                             'Tekst artykułu': text_of_article,
                             'Adnotacja': 'nota o autorze/autorce' if about_author else None,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
sitemap_links = ['http://www.impresjee.pl/post-sitemap.xml', 'http://www.impresjee.pl/post-sitemap2.xml']

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, sitemap_links),total=len(sitemap_links)))


all_results = []
with ThreadPoolExecutor(max_workers=4) as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')

   

with open(f'data\\impresjee_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\impresjee_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   