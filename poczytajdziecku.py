#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import time
from functions import date_change_format_long


#%% def
def get_articles_links(sitemap_link):   
    html_text_sitemap = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)   
    

def dictionary_of_article(article_link):
    # article_link = 'https://www.poczytajdziecku.pl/2023/10/ksiazki-na-halloween-2023.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/12/podpowiednik-swiateczny-zabojstwo.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/02/pola-i-buster.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/09/co-robia-uczucia.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/05/ksiazkowy-ulf-to-ja.html'
    # article_link = 'https://www.poczytajdziecku.pl/2021/05/basia-wielka-ksiega-o-uczuciach.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/11/jedyna-taka-ester.html'
    # article_link = 'https://www.poczytajdziecku.pl/2020/08/pate.html'
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        author = soup.find('span', {'itemprop': 'name'}).text
        author = re.search(r'.*(?<=\/)', author).group(0).replace('/', '')
    except(TypeError, AttributeError):
        author = None
        
    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    except AttributeError:
        title_of_article = None
        
    
    try:
        date_of_publication = soup.find('h2', class_='date-header').text
        date_of_publication = date_change_format_long(date_of_publication)
    except AttributeError:
        date_of_publication = None
    
    
    try:
        article = soup.find('div', class_='post-body entry-content')
    except AttributeError:
        article = None
        
    try:    
        text_of_article = " ".join([x.text.strip() for x in article.find_all('div', class_='MsoNormal')]).replace("\n", " ")
    except AttributeError:
        text_of_article = None
        
    if text_of_article == None or text_of_article == "":
        
        try:    
            text_of_article = article.text.replace("\n", "").strip()
        except AttributeError:
            text_of_article = None
    
    try:
        book_description = [x.text for x in article.find_all('div', class_='MsoNormal') if x != " "]
        book_description = [x.replace('\n', '').strip() for x in book_description if x != '\n']
        # book_description = [x for x in book_description if x != ''][-1]
        book_description = " ".join([x for x in book_description if re.search('(\d\.$)|\+', x)])
    except (AttributeError, KeyError, IndexError, TypeError):
        book_description = None  
        
    if book_description == None or book_description == "":
        try:
            book_description = [x.text for x in article.find_all('p', class_='MsoNormal') if x != " " ]
            book_description = [x.replace('\n', ' ').replace('\xa0', '').strip() for x in book_description]
            book_description = " ".join([x for x in book_description if re.search('(\d\.$)|\+', x)])
        except (AttributeError, KeyError, IndexError, TypeError):
            book_description = None
        

    try:   
        author_of_book = re.findall(r'^[\p{L}\s\-\.]+(?=\,)', book_description)[0].strip()
    except (AttributeError, KeyError, IndexError, TypeError):
        author_of_book = None  
        
    try:    
        title_of_book = " | ".join(re.findall(r'„.*”', book_description)).replace('”,', '” |')
    except (AttributeError, KeyError, IndexError, TypeError):
        title_of_book = None   
        
        
    try:
        publisher = re.search(r'wyd.*,', book_description).group(0).replace(',', '')
    except (AttributeError, KeyError, IndexError, TypeError):
        publisher = None
    
    try:
        place_and_year = re.search(r'[A-Z\p{L}][a-z\p{L}]+\s\d{4}–?\d?\d?\d?\d?', book_description).group(0)  
    except (AttributeError, KeyError, IndexError, TypeError):
        place_and_year = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'poczytajdziecku|blogger', x)])
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
                             'Opis książki': book_description, 
                             'Autor książki': author_of_book, 
                             'Tytuł książki': title_of_book,
                             'Wydawnictwo': publisher, 
                             'Rok i miejsce wydania': place_and_year, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        
            
    all_results.append(dictionary_of_article)

#%% main
articles_links = []
sitemap_link = get_articles_links('https://www.poczytajdziecku.pl/sitemap.xml')

    
#306 artykułów
#Usunięcie zdublowanych:
articles_links = list(dict.fromkeys(articles_links)) 


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
df = df.dropna(subset=["Data publikacji"]) #Usunięcie dwóch linków bez daty (podstrony)


with open(f'data\\poczytajdziecku_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

with pd.ExcelWriter(f"data\\poczytajdziecku_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



