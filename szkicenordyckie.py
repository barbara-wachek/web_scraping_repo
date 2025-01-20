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
    
    articles_links = [x.text for x in soup.find_all('loc') if re.match(r'https:\/\/szkicenordyckie\.pl\/\d{4}.*',x.text)]
    
    return articles_links

def dictionary_of_article(article_link):  
    # article_link = 'https://szkicenordyckie.pl/2006/05/miasteczko-salem/'
    # article_link = 'https://szkicenordyckie.pl/2009/09/feministyczne-porno/'
    # article_link = 'https://szkicenordyckie.pl/2019/01/bullerbyn-istnieje-naprawde/'
    # article_link = 'https://szkicenordyckie.pl/2014/04/margit-sandemo-autorka-slynnej-sagi-o-ludziach-lodu-obchodzi-dzisiaj-90-urodziny/'
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date = soup.find('li', class_="post-date meta-wrapper").find('span', class_='meta-text').a.text.strip()
        date_of_publication = date_change_format_short(date)
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
    try:
        author = soup.find('li', class_="post-author meta-wrapper").find('span', class_='meta-text').a.text.strip()
    except:
        author = None

    try:
        category = soup.find('div', class_='entry-categories-inner').text.strip()
    except:
        category = None


    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([e.text for e in article.find_all('p')]).strip()  
    except:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text.strip() for x in soup.find('li', class_='post-tags meta-wrapper').find('span', class_='meta-text').find_all('a')])
    except:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'szkicenordyckie|#', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([article_link + x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Kategoria': category,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main

sitemap_link = 'https://szkicenordyckie.pl/post-sitemap.xml'
articles_links = get_articles_links(sitemap_link)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values(by='Data publikacji')
   

with open(f'data\\szkicenordyckie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\szkicenordyckie_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   