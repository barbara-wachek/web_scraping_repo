
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

def get_article_links(sitemap_url): 
    html_text = requests.get(sitemap_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    links = [e.text for e in soup.find_all('loc') if re.match(r'https\:\/\/jaroslawpluciennik.com\/\d{4}\/\d{2}\/\d{2}\/.*', e.text)]
    return links

def dictionary_of_article(article_link):  
    
    #article_link = 'https://jaroslawpluciennik.com/2026/01/18/bjork-jako-ikona-pragmatycznego-aktywizmu-dlaczego-national-geographic-umiescil-artystke-na-okladce/'

 
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')

    
    try:
        title_of_article = soup.find('h1', class_='entry-title').get_text(strip=True)
    except:
        title_of_article = None
  
    author = 'Jarosław Płuciennik'
        
    try:
        date_of_publication = soup.find('time').get('datetime')[:10]
    except:
        date_of_publication = None    
        

    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p', class_='wp-block-paragraph')])
    except:
        text_of_article = None
    

        
    try:
        category = " | ".join([x.text for x in soup.find('span', class_='cat-links').find_all('a')])
    except:
        category = None
        
   
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_="tags-links").find_all('a')])
   
    except:
        tags = None
    
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'jaroslaw', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        

        
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main

article_links = get_article_links("https://jaroslawpluciennik.com/sitemap.xml")
print(f"Znaleziono {len(article_links)} linków.")


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)

df.to_json(f'data/jaroslawpluciennik_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/jaroslawpluciennik_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   