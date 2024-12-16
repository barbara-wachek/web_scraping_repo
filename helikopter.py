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
        issue_number = soup.find('span', class_='link-cat').text   
    except:
        issue_number = None
        
        
    try:
        title_of_article = soup.find('h1', class_='site-hero-title helikopter-title').text.strip()
    except:
        title_of_article = None
        
        
    try:
        author = soup.find('h2', class_="h5 h-bold x-line").text.strip()
    except:
        author = None

   

    article = soup.find('div', class_='article post-content')
    
    
    try:
        poems = " | ".join([poem.text for poem in article.find_all('h2')])
    except:
        poems = None
    
    try:
       text_of_article = " ".join([e.text for e in article.find('div', class_='no-hyphens')]).strip()  
    except:
        text_of_article = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'helikopter|opt-art|#', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Numer czasopisma': issue_number,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tytuły utworów': poems,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main

link = 'https://opt-art.net/helikopter/'
sitemap_links = ['https://opt-art.net/helikopter-sitemap1.xml', 'https://opt-art.net/helikopter-sitemap2.xml', 'https://opt-art.net/helikopter-sitemap3.xml']


articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, sitemap_links),total=len(sitemap_links)))

#usuniecie linkow do spisow tresci
articles_links = [e for e in articles_links if re.match(r'https:\/\/opt-art\.net\/helikopter\/\d*-?\d*-\d{4}\/.+', e)]


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()

   
with open(f'data\\helikopter_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\helikopter_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   