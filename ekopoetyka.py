#%%import
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
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def
def get_art_links(start_url): 
    output = []
    i = 1
    while True:
        url = start_url + str(i) + '/'
        response = requests.get(url)
        if response.ok:
            soup = BeautifulSoup(response.text, 'lxml')
            posts = soup.find_all('div', class_='row limit-width')
            links = [(e.find('a')['href'],
                      e.find('span', class_='eko-post-date').text,
                      e.find('span', class_='eko-post-title').text) for e in posts]
            output.extend(links)
        else: break
        i += 1    
    return output


def dictionary_of_article(article_tuple):
    article_link, date, title_of_article = article_tuple

    response = requests.get(article_link)
    if not response.ok:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')

    # author
    author = None
        
    # date
    date = datetime.strptime(date, '%d.%m.%Y')
    date = date.strftime('%Y-%m-%d')

    # content
    if art_content := soup.find('div', class_='post-content'):
        text_of_article = ' '.join([p.text for p in art_content.find_all('p')])
        article = text_of_article.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
start_url = 'http://ekopoetyka.com/category/blog/page/'
article_links = get_art_links(start_url)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))

with open(f'data/ekopoetyka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/ekopoetyka_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   