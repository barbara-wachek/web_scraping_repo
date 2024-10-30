#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import time


#%% def
def get_articles_links(sitemap_link):
    html_text_sitemap = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [x.text for x in soup.find_all('loc')]
    articles_links.extend(links)


def dictionary_of_article(article_link):  
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    try:
        date_of_publication = soup.find('time', class_='entry-date published')['datetime']
        date_of_publication = re.search(r'^\d{4}-\d{2}-\d{2}', date_of_publication).group(0)
    except AttributeError:
        date_of_publication = None
        
    try:   
        author = soup.find('span', class_='author vcard').a['title']
    except AttributeError: 
        author = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except AttributeError:
        title_of_article = None      
        
    try:
        category = " | ".join([x.text for x in soup.find('span', class_='cat-links').find_all('a')])
    except:
        category = None
 
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='tag-links').find_all('a')])
    except: 
        tags = None
    
    
    try:
        article = soup.find('div', class_='article-content clearfix')
    except AttributeError:
        article = None
        
    try:    
        text_of_article = " ".join([x.text.strip() for x in article.find_all('p')]).strip()
    except AttributeError:
        text_of_article = None
        

    try:    
        title_of_book = " | ".join([x for x in re.findall(r'\„[\p{L}\s\-\,\.\)\(\?\!)\d\–]*\”', title_of_article)])
    except (AttributeError, KeyError, IndexError, TypeError):
        title_of_book = None           
    
    
    if title_of_book == None or title_of_book == '':
        try:
            title_of_book = re.search(r'.*(?=\s–\s) ', title_of_article).group(0)
        except (AttributeError, KeyError, IndexError, TypeError):
            title_of_book = None      
       
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'wnaszejbajce', x)])
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
                             'Kategorie': category,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Tytuł książki': title_of_book,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        
            
    all_results.append(dictionary_of_article)

#%% main
 
sitemap_links = ['https://wnaszejbajce.pl/wp-sitemap-posts-post-1.xml', 'https://wnaszejbajce.pl/wp-sitemap-posts-post-2.xml']

articles_links  = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, sitemap_links ),total=len(sitemap_links)))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data\\wnaszejbajce_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

with pd.ExcelWriter(f"data\\wnaszejbajce_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    













