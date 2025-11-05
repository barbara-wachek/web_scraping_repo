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

def get_article_links(sitemap_links):
    
    if isinstance(sitemap_links, str):  # jeśli to pojedynczy link
        sitemap_links = [sitemap_links]  # zamień na listę

    all_article_links = []
    for sitemap_link in sitemap_links:
        html_text = requests.get(sitemap_link).text
        soup = BeautifulSoup(html_text, 'xml')
        links = [e.text for e in soup.find_all('loc')]
        all_article_links.extend(links)
        
    return all_article_links



def dictionary_of_article(article_link, max_retries=5, delay=2):
    
    # article_link = 'http://jakobe.art.pl/kore/'
    # article_link = 'http://jakobe.art.pl/tintin/'
    # article_link = 'http://jakobe.art.pl/grypa-dzien-8/'

    retries = 0
    response = None

    while retries < max_retries:
        try:
            response = requests.get(article_link, timeout=10)
            if 'Error 503' not in response.text and response.status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass  # np. timeout, connection error
        
        retries += 1
        time.sleep(delay)

    if response is None or not response.text.strip():
        print(f"❌ Nie udało się pobrać: {article_link}")
        return None

    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'lxml')

    # response = requests.get(article_link)

    # while 'Error 503' in response.text:
    #     time.sleep(2)
    #     response = requests.get(article_link)
    
    # # Wymuś poprawne kodowanie
    # response.encoding = 'utf-8'
    
    # soup = BeautifulSoup(response.text, 'lxml')
    
    
    try: 
        raw_date = soup.find('span', class_='post-date').text.strip()
        date_obj = datetime.strptime(raw_date, '%d/%m/%Y')
        date_of_publication = date_obj.strftime('%Y-%m-%d')
    except: 
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='post-title').text.strip()
    except: 
        title_of_article = None
        
    try:
        author = soup.find('span', class_='post-author').text
        if author == 'jakobe': 
            author = 'Jakobe Mansztajn'
    except:
        author = None

    
    article = soup.find('div', class_='post-content')  
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')]).replace('\xa0', ' ')
    except:
        text_of_article = None
    
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'jakobe', x)])
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
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
all_article_links = get_article_links('http://jakobe.art.pl/post-sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_article_links),total=len(all_article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/jakobe_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/jakobe_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     
   
    


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    