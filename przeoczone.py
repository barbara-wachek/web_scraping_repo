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
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.text for e in soup.find_all('loc')]
        all_article_links.extend(links)
        
    return all_article_links



def dictionary_of_article(article_link):
    
    # article_link = 'https://przeoczone.pl/tworzyc-sobie-zycia/'
    # article_link = 'https://przeoczone.pl/niezbadane-terytoria/'
    # article_link = 'https://przeoczone.pl/literatura-piekna-umiera/'

    
    response = requests.get(article_link)

    while 'Error 503' in response.text:
        time.sleep(2)
        response = requests.get(article_link)
    
    # Wymuś poprawne kodowanie
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    
    try: 
        date_of_publication = soup.find('time')['datetime'][:10]
    except: 
        date_of_publication = None
    
   
    try:
        title_of_article = soup.find('h2', class_='wp-block-post-title has-huge-font-size').text.strip()
    except: 
        title_of_article = None
        
    try:
        author = soup.find('div', class_='wp-block-post-author-name').a.text
    except:
        author = None

    try:
        tags = [x.text for x in soup.find('div', class_='taxonomy-category wp-block-post-terms').find_all('a')]
    except:
        tags = None

    
    article = soup.find('div', class_='wp-block-column is-layout-flow wp-block-column-is-layout-flow')  
    

    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')]).replace('\xa0', ' ')
    except:
        text_of_article = None
    
        
    #Popraw branie tekstow. Pierwszy link nie zwraca tekstu w calosci. Zbadaj

        
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'obszaryprzepisane', x)])
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
                             'Tags': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x.get('src') for x in article.find_all('img')] else False,
                             'Filmy': True if [x.get('src') for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
all_article_links = get_article_links('https://przeoczone.pl/post-sitemap.xml')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_article_links),total=len(all_article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/przeoczone_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/przeoczone_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     
   
    


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    