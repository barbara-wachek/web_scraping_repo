import requests
from bs4 import BeautifulSoup

import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time


#%%def

def get_article_links(category_link, articles_per_page=18):
    all_links = []
    start = 0

    while True:
        url = f"{category_link}?start={start}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Nie udało się pobrać strony start={start}")
            break

        soup = BeautifulSoup(response.text, 'lxml')
        items = soup.find_all('div', class_="blog-item")
        if not items:  # brak kolejnych artykułów → koniec
            break

        links = ['https://tekstyzrodlowe.pl' + e.a.get('href') for e in items]
        all_links.extend(links)

        start += articles_per_page  # przejście do kolejnej strony

    return all_links


def dictionary_of_article(article_link):  
    
    # article_link = 'https://tekstyzrodlowe.pl/teatr/28-urodziny-czarnego-kota-rudego.html'
    # article_link = 'https://tekstyzrodlowe.pl/teatr/dom-niespokojnej-starosci.html'
 
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    
    try:
        title_of_article = soup.find('h1').get_text(strip=True)
    except:
        title_of_article = None
  
    author = 'Daniel Źródlewski'
        
    article = soup.find('div', class_='com-content-article__body')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    
    category = re.sub(r'(https\:\/\/tekstyzrodlowe\.pl\/)(\w*)(\/.*)', r'\2', article_link)
        
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'html$', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        

        
    dictionary_of_article = {'Link': article_link,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main

teatr_links = get_article_links("https://tekstyzrodlowe.pl/teatr.html")
literatura_links = get_article_links("https://tekstyzrodlowe.pl/literatura.html")

article_links = teatr_links + literatura_links


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()

df.to_json(f'data/tekstyzrodlowe_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/tekstyzrodlowe_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   