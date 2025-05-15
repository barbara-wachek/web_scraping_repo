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
from functions import date_change_format_long



#%% def    
def get_articles_links(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'xml')
    sitemap_links = [e.text for e in soup.find_all('loc') if re.match(r'https:\/\/bookowscy\.wordpress\.com\/\d{4}\/\d{2}\/\d{2}\/.+', e.text)]
    return sitemap_links
    
    
def dictionary_of_article(article_link):
    # article_link = 'https://bookowscy.wordpress.com/2017/08/19/puszcza-pamieta/'
    # article_link = 'https://bookowscy.wordpress.com/2021/04/19/casus-czerwonego-kapturka/'
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')


    try:
        date_of_publication = soup.find('time')['datetime'][:10]
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
    
    try:
        author = soup.find('a', class_='url fn n').text 
    except:
        author = None
    
        
    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = article.text.replace('\n', ' ').replace("  ", " ")
    except:
        text_of_article = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='tags-links').find_all('a')])
    except:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'bookowscy', x)])
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
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
    

    all_results.append(dictionary_of_article)


#%% main
articles_links = get_articles_links('https://bookowscy.wordpress.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/bookowscy_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
   
with pd.ExcelWriter(f"data/bookowscy_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   


   
   
   
   
   
   
   
   
   