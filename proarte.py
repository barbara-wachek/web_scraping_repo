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
def get_art_links(url):
    output = []
    i = 1
    while True:
        page_url = url + f'page/{i}/'
        resp = requests.get(page_url)
        if not resp.ok:
            break
        soup = BeautifulSoup(resp.text, 'lxml')
        links = [e.find('a')['href'] for e in soup.find_all('h2', class_='entry-title')]
        output.extend(links)
        i += 1
    return output


def dictionary_of_article(article_link):
    
    response = requests.get(article_link)
    if not response.ok:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    # title
    if title_of_article := soup.find('h1', class_='entry-title'):
        title_of_article = title_of_article.text
    else:
        title_of_article = None

    # author
    if author_div := soup.find('div', class_='entry-author'):       
        author = author_div.text
    else:
        author = None
        
    # date
    if date_div := soup.find('div', class_='entry-date'):
        date = date_div.find('a').text
        date = datetime.strptime(date, '%d/%m/%Y')
        date = date.strftime('%Y-%m-%d')
    else:
        date = None
        
    # category
    if cat_div := soup.find('div', class_='entry-cats'):
        category = [e.text for e in cat_div.find_all('a')]
        category = ' | '.join(category)
    else:
        category = None
    
    # number
    if tag_div := soup.find('div', class_='entry-tags'):
        number = tag_div.find('a').text
    else:
        number = None
    
    
    # content
    if art_content := soup.find('div', class_='entry-content'):
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
                             'Kategoria': category,
                             'Numer': number,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
articles_links = set()
webpage_url = 'http://proarte.net.pl/category/'

for category in tqdm(['literatura', 'wywiad', 'poezja', 'szkic', 'teatr', 'kino', 'relacja']):
    url = webpage_url + f'{category}/'
    articles_links.update(get_art_links(url))
articles_links = list(articles_links)   

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/proarte_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/proarte_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

