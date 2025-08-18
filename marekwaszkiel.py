#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time
from functions import date_change_format_short


#%% def    
def get_article_links(link):
    format_link = re.search(r'https\:\/\/www\.marekwaszkiel\.pl\/category\/blog\/page\/', link).group(0)
    
    article_links = []
    
    for x in range(1, 20): 
        page_link = format_link + str(x)
        html_text = requests.get(page_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.find('a').get('href') for e in soup.find_all('h1', class_='post-title')]
        if links != []:
            article_links.extend(links)
        
    return article_links




def dictionary_of_article(article_link):   
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    
    author = 'Marek Waszkiel'
    
    try:
        date = soup.find('span', class_='meta-date').text
        date_of_publication = date_change_format_short(date)
    except:
        date_of_publication = None
        

    try:
        title_of_article = soup.find('h1', class_='post-title').get_text(strip=True)
    except: 
        title_of_article = None
    
    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.get_text(strip=True) for x in article.find_all('p')])
    except:
        text_of_article = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='meta-tags').find_all('a')])
    except:
        tags = None
        
    try:
        category = " | ".join([x.text for x in soup.find('div', class_='meta-categories').find_all('a')])
    except:
        category = None

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'marekwaszkiel', x)])
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
                             'Kategoria': category,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool(article and article.find_all('img')),
                             'Filmy': bool(article and article.find_all('iframe')),
                             'Linki do zdjęć': photos_links
                             }
    
    all_results.append(dictionary_of_article)
    
    return all_results
 
#%% main

article_links = get_article_links('https://www.marekwaszkiel.pl/category/blog/page/11')


all_results = []   
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(dictionary_of_article, article_links),total=len(article_links)))

  
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/marekwaszkiel_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/marekwaszkiel_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    