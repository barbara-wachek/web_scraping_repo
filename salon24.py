
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


def dictionary_of_article(article_link):  
    
    article_link = 'https://www.salon24.pl/u/wspomnienia/859744,epilog-raz-jeszcze'

 
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    
    try:
        header = soup.find('div', class_=" article-header--bloger")
    except:
        header = None

    
    try:
        title_of_article = soup.find('div', class_='article-header--content').h1.text
    except:
        title_of_article = None
  

    author = 'Zofia Pawłowska'
        
    try:
        author = soup.find('div', class_='article-header--info').find('a', class_="nick")
    
        
    try:
        tags = " | ".join([x.text for x in header.find('span', class_="tags-links").find_all('a')])
      
    except:
        tags = None  
         
        
    try:
        date_of_publication = soup.find('time').get('datetime')[:10]
    except:
        date_of_publication = None    
        

    article = soup.find('div', class_='article-content')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    

    
        
   
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
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main

subpages_links = []
for number in range(1,8): 
    link = f'https://www.salon24.pl/u/wspomnienia/posts/0,{str(number)},wszystkie'
    subpages_links.append(link)
    
articles_links = []

for link in subpages_links:
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    links = ["https:" + x.a['href'] for x in soup.find_all('h3', class_='title')]
    articles_links.extend(links)





all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)

df.to_json(f'data/jaroslawpluciennik_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/jaroslawpluciennik_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   