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

def get_article_links(sitemap_link):

    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    # article_link = 'https://czytanieisluchanie.blogspot.com/2025/05/obszerne-obserwowanie.html'
    # article_link = 'https://czytanieisluchanie.blogspot.com/2025/04/kwestia-gramatyki.html'
    article_link = 'https://czytanieisluchanie.blogspot.com/2024/03/w-srodku-byocicho.html'
    # article_link = 'https://czytanieisluchanie.blogspot.com/2022/11/przymknijcie-oczy-otworzcieserca.html'
    # article_link = 'https://czytanieisluchanie.blogspot.com/2019/08/woski-sen.html'
    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        author = soup.find('span', class_='post-author-label').text.strip()
    except:
        author = None
    
    if author == 'Posted by BooksJarkaHoldena':
        author = 'Jarek Holden'


    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    except: 
        title_of_article = None
        
        
    try:
        date_of_publication = soup.find('a', class_='timestamp-link').time['datetime'][:10]
    except:
        date_of_publication = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='byline post-labels').find_all('a', {'rel':'tag'})])
    except:
        tags = None
    
    article = soup.find('div', class_='post-body-container')
    
    try:
        text_of_article = article.text.replace("\n", " ").replace("  ", " ").strip().replace('\xa0', ' ')
    except:
        text_of_article = None
    
    
    pattern = re.compile(
    r"""(?x)                                    # tryb wieloliniowy z komentarzami
    (
        [„"”]?[A-ZĄĆĘŁŃÓŚŹŻ][^.!?\n]{3,150}?     # tytuł zaczynający się wielką literą (z opcjonalnym cudzysłowem)
        [.!?,]\s*                                # zakończenie tytułu i separator
        (?:[^.\n]*\.\s*)*?                       # kolejne zdania – autor, przekład, wydawnictwo itp.
        \d{1,2},?\d{0,2}/10                      # ocena w formacie np. 10/10 lub 7,5/10
    )
    """,
    flags=re.UNICODE
)
    
    
    try:
        matches = pattern.findall(text_of_article)
        for match in matches:
            book_description = match.strip()
            
    except:
        book_description = None
        
        
    if book_description:
        
        pattern = re.compile(
            r"""(?x)
            ^\s*
            [„"”]?
            (?P<title>[^".„”]+?)       # tytuł: do pierwszej kropki/cudzysłowu
            [".„”]?[.,]?\s+
            (?P<author>[^.,\n]+?)      # autor: do pierwszej kropki/przecinka
            [.,]                       # separator po autorze
            """
        )
        
        match = pattern.search(book_description)
        
        if match:
            try:
                title_of_book =  match.group("title").strip()
            except:
                title_of_book = None
            
            try:
                author_of_book = match.group("author").strip()
            except:
                author_of_book = None

        
        
        
    
    try:
        title_of_book = " | ".join(re.findall(r"(?P<tytul>.+?)\s+-\s+[A-ZŻŹĆŁÓŚŃ][a-ząćęłńóśżź]+(?:\s+[A-ZŻŹĆŁÓŚŃ][a-ząćęłńóśżź]+)+"
, text_of_article))
    except:
        title_of_book = None
        
    try: 
        author_of_book = " | ".join(re.search(r'(?:[-–.]\s+)(?P<autor>[A-ZŻŹĆŁÓŚŃ][a-ząćęłńóśżź]+(?:\s+[A-ZŻŹĆŁÓŚŃ][a-ząćęłńóśżź]+)+)', text_of_article))
    except:
        author_of_book = None
    
    
    
    
    
    
    
    
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|coczytamkonstantemu', x)])
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
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book, 
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('https://czytanieisluchanie.blogspot.com/sitemap.xml')
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'czytanieisluchanie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"czytanieisluchanie_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   



   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    