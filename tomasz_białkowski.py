#%% import
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import mktime
import json
from functions import date_change_format_long

#%% def
def tomaszbialkowski_web_scraping_sitemap(sitemap):
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
     
    date_of_publication = date_change_format_long(soup.find('h2', class_='date-header').text)


    content_of_article = soup.find('div', class_='post-body entry-content')
    
    tags_span = soup.find('span', class_='post-labels').findChildren('a')
    if tags_span:
        tags = " | ".join([tag.text for tag in tags_span])
    else:
        tags = None
  
    author = soup.find('span', attrs={'itemprop':'name'})
    if author:
        author = author.text
    else:
        author = None


    text_of_article = content_of_article.text.strip().replace('\n', '')
 
    title_of_article = soup.find('h3', class_='post-title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(blogspot)|(jpg)', x)])
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
        photos_links = None
        



    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                             }

    all_results.append(dictionary_of_article)
    
    
    
#%% main

sitemap_links = tomaszbialkowski_web_scraping_sitemap('http://tomaszbialkowski.blogspot.com/sitemap.xml')
articles_links = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))



with open(f'tomasz_bialkowski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

    
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)

    
with pd.ExcelWriter(f"tomasz_bialkowski_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posty', index=False)    
    
    
    
    
    
    
    
    
    
    
    
    
        
        