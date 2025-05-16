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
from functions import date_change_format_short



#%% def    
def get_articles_links(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'xml')
    sitemap_links = [e.text for e in soup.find_all('loc') if re.match(r'https:\/\/czytelniaweb\.wordpress\.com\/\d{4}\/\d{2}\/\d{2}\/.+', e.text)]
    return sitemap_links
    
    
def dictionary_of_article(article_link):
    # article_link = 'https://czytelniaweb.wordpress.com/2023/02/07/brian-porter-szucs-wiara-i-ojczyzna/'
    # article_link = 'https://czytelniaweb.wordpress.com/2022/07/16/barbara-sadurska-czarny-hetman/'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')


    try:
        date = soup.find('span', class_='post-meta-date').a.text
        date_of_publication = re.sub(r"(\d{1,2}\s+\p{L}+),\s+(\d{4})", r"\1 \2", date)
        date_of_publication = date_change_format_short(date_of_publication)
    except:
        date_of_publication = None
        
    if date_of_publication == None:
        match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', article_link)
        if match:
            date_of_publication = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            
    
    try:
        title_of_article = soup.find('h1', class_='post-title').text.strip()
    except:
        title_of_article = None
    
    try:
        author = soup.find('span', class_='post-meta-author').a.text
    except:
        author = None
    
    try:
        category = " | ".join([x.text for x in soup.find('p',class_='post-categories').find_all('a')])
    except:
        category = None
        
    # article = soup.find('div', class_='post-content')
    article = soup.find('article')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
        
    try:
        title_of_book = " | ".join([x.replace('\xa0', ' ') for x in re.findall(r'„(.+?)”', title_of_article)])
    except:
        title_of_book = None
          
    try:
        author_of_book = " | ".join([x for x in re.findall(r'^(.*?)\s+–\s+', title_of_article)])
    except:
        author_of_book = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'czytelniaweb', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None 
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
    

    all_results.append(dictionary_of_article)


#%% main
articles_links = get_articles_links('https://czytelniaweb.wordpress.com/sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/czytelniaweb_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values(by='Data publikacji', ascending=True)
   
with pd.ExcelWriter(f"data/czytelniaweb_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   


   
   
   
   
   
   
   
   
   