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
from functions import date_change_format_long



#%% def    
#Na mapie strony dostępne są tez linki do tagów, kategorii o autorze. Pomijam to, bo ze informacje (Tagi) wyciagam przy okazji opracowywania artykułow. Czasami wyskakuje Connection Error

def get_articles_links(posts_sitemap):
    posts_sitemap = 'https://biblioteczka-apteczka.pl/post-sitemap.xml'
    #Trzeba ustawić User-Agent, bo jest blokada przeciwko botom. Bez tego zwraca bląd 403. 
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    
}
    
    html_text = requests.get(posts_sitemap, headers=headers).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    print(html_text)
    links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(links)
    return links

    
def dictionary_of_article(article_link):
    # article_link = 'https://biblioteczka-apteczka.pl/2016/08/14/bo-ja-ide-do-szpitala/'
    # article_link = 'https://biblioteczka-apteczka.pl/2017/06/25/anielka-i-wyjazd-rodzicow/'
    # article_link = 'https://biblioteczka-apteczka.pl/2019/03/25/wszedzie-i-we-wszystkim/'
    
    # article_link = 'https://biblioteczka-apteczka.pl/2022/03/27/siedem-szczesliwych/'
    # article_link = 'https://biblioteczka-apteczka.pl/2023/02/22/zostan-sama-w-domu/'
    # article_link = 'https://biblioteczka-apteczka.pl/2020/11/06/pustka/'
    
    
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
}
    
    html_text = requests.get(article_link, headers=headers).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    date_of_publication = re.sub(r'(.*)(\d{4})(\/)(\d{2})(\/)(\d{2})(.*)', r'\2-\4-\6', article_link)
    
    try:
        title_of_article = re.search(r'.*(?=\s-\sBiblioteczka-apteczka$)', soup.find('title').text).group(0)
    except AttributeError:
        title_of_article = None
        
    if title_of_article == None or title_of_article == '':
        try: 
            title_of_article = soup.find('h1', class_='post-title single-post-title entry-title').text
        except AttributeError:
            title_of_article = None
            
            
    tags = " | ".join([x.text for x in soup.find_all('a', {'rel':'tag'})])
   
    
    article = soup.find('div', class_='inner-post-entry entry-content')
    
    try:
        text_of_article = article.text
        # text_of_article = re.search(r'(?<=\d\,\dK)(.*)', text_of_article).group(0)
    except (AttributeError, IndexError):
        text_of_article = None
    
    
    
    
    try:
        book_description = re.sub(r'(^[\d\.\,]*K)(.*\d\+)(.*)', r'\2', text_of_article)
    except (AttributeError, TypeError):
        book_description = None
    
    author = 'Iwona Czesiul-Budkowska'
    
    try: 
        title_of_book = re.search(r'^[A-Za-z\p{L}\s]*', book_description).group(0)
    except (IndexError, AttributeError, TypeError):
        title_of_book = None
    
    
    if title_of_book == None or title_of_book == '':
        try: 
            title_of_book = re.search(title_of_article.lower(), book_description.lower()).group(0)
        except (AttributeError, TypeError):
            title_of_book = None
            
    try:
        author_of_book = re.search(r'(?<=\,)([\p{L}\s\'\-]*)(\,.*)', book_description).group(0).strip()
        author_of_book = re.sub(r'([\p{L}\s\'\-]*)(\,.*)', r'\1', author_of_book)
    except (IndexError, AttributeError, TypeError):
        author_of_book = None
        
    try:
        publisher = re.search(r'Wydawnictwo\s[\p{L}\s\-\']*', book_description).group(0).strip()
    except (IndexError, AttributeError, TypeError):
        publisher = None
    
    try:
        year_of_publishing = re.search(r'2\d\d\d', book_description).group(0).strip()
    except (IndexError, AttributeError, TypeError):
        year_of_publishing = None
        
        
    try:
        isbn = re.search(r'ISBN:\s[\d\-]*', book_description).group(0)
    except (IndexError, AttributeError, TypeError):
        isbn = None
    

    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'biblioteczka-apteczka|apteczka', x)])
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
                              'Opis książki': book_description,
                              'Autor książki': author_of_book,
                              'Tytuł książki': title_of_book,
                              'Wydawnictwo': publisher,
                              'Rok wydania': year_of_publishing,
                              'ISBN': isbn, 
                              'Linki zewnętrzne': external_links,
                              'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
sitemap_link = 'https://biblioteczka-apteczka.pl/sitemap_index.xml'

articles_links = []
articles_links = get_articles_links('https://biblioteczka-apteczka.pl/post-sitemap.xml')
  
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
#usunięcie zbędnego wiersza:
df = df[df['Data publikacji'] != 'https://biblioteczka-apteczka.pl/']
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
    
with open(f'data\\biblioteczkaapteczka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data\\biblioteczkaapteczka_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive




   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    