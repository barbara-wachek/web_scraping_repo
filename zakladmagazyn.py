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


#%% def    

def get_article_links(sitemap_link):

    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):

    # article_link = 'https://www.zakladmagazyn.pl/post/wolny-hamkało-kawalec-konieczny-performens-numeryczność'
    # article_link = 'https://www.zakladmagazyn.pl/post/justyna-wysocka-zestaw-dwóch-wierszy'
    # article_link = 'https://www.zakladmagazyn.pl/post/hanna-janczak-jeden-wiersz'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    try:
        title_of_article = soup.find('h1', class_='H3vOVf').text.strip()
    except: 
        title_of_article = None
        
    try:
        author = soup.find('a', class_='g-profile').span.text
    except:
        author = None
    
    
    
    try:
        pattern = re.compile(r'^(.*)\s-\s')
        author = pattern.match(title_of_article).group(1).replace('/', ' | ')
    except:
        author = None
     
    date_of_publication = None    
    try:    
        json_ld_scripts = soup.find_all('script', type='application/ld+json')

        for script in json_ld_scripts:

                data = json.loads(script.string)
                # sprawdź, czy to jest wpis blogowy
                if data.get('@type') == 'BlogPosting':
                    date_str = data.get('datePublished') 
                    date_of_publication = datetime.fromisoformat(date_str.replace('Z', '')).date()
    except:
        date_of_publication = None
        


    article = soup.find('div', class_='WZmlO')
    
    try:
        text_of_article = article.text
    except:
        text_of_article = None
    

    try:
        title_of_poem = " | ".join([x.text for x in article.find_all('strong')])
    except:
        title_of_poem = None

    try:
        tags_list = [x.text for x in soup.find('ul', class_='zmug2R').find_all('a')]
        tags = " | ".join(tags_list)
    except:
        tags = None

    try:
        issue = " ".join([x for x in tags_list if 'numer' in x])
    except: 
        issue = None

   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'zakladmagazyn', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Numer': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tytuł wiersza': title_of_poem,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool([x.get('src') for x in article.find_all('img')]) if article else False,
                             'Filmy': bool([x.get('src') for x in article.find_all('iframe')]) if article else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('https://www.zakladmagazyn.pl/blog-posts-sitemap.xml')
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date.apply(lambda x: x.isoformat())
df = df.sort_values('Data publikacji', ascending=True)

json_data = df.to_dict(orient='records')

with open(f'data/zakladmagazyn_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/zakladmagazyn_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    