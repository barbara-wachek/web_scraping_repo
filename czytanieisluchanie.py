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
    #article_link = 'https://czytanieisluchanie.blogspot.com/2025/04/kwestia-gramatyki.html'
    # article_link = 'https://czytanieisluchanie.blogspot.com/2024/03/w-srodku-byocicho.html'
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
    
    
    try:
        book_description_end = re.search(r"\d{4}\.?", text_of_article).end()
        book_description = text_of_article[:book_description_end]
    except:
        book_description = None

    author_of_book = None
    if book_description:
        patterns = [
            # 1. Tytuł w cudzysłowie, potem autor i liczba stron
            r'["”]{1,2}.*?["”]{1,2}\.?\s*([A-ZŻŹĆĄŚĘŁÓŃ][^,\.]+?),\s*\d+\s*stron',
    
            # 2. Autor po ostatniej kropce w tytule (wielokrotne zdania w tytule)
            r'\.\s*([A-ZŻŹĆĄŚĘŁÓŃ][^,]+?),\s*\d+\s*stron',
    
            # 3. Po cudzysłowie i kropce (np. „Tytuł”. Autor.)
            r'[”"]\s*\.?\s*([A-ZŻŹĆĄŚĘŁÓŃ][^.,;\(\)]+?)(?=\.\s*(Przekład|tłum|redakcja|Wydawnictwo|$))',
    
            # 4. Po tytule zakończonym kropką (np. Tytuł. Autor.)
            r'^[^\.]+?\.\s*([A-ZŻŹĆĄŚĘŁÓŃ][^.,;\(\)]+?)(?=\.\s*(Przekład|tłum|redakcja|Wydawnictwo|$))',
    
            # 5. Po myślniku (np. Tytuł - Autor, ...)
            r'-\s*([A-ZŻŹĆĄŚĘŁÓŃ][^,;\.\(\)]+)',
    
            # 6. Po tytule z nawiasem (np. Tytuł (język), Autor.)
            r'\)\s*,?\s*([A-ZŻŹĆĄŚĘŁÓŃ][^.,;\(\)]+?)(?=\.\s*(Przekład|tłum|redakcja|Wydawnictwo|$))',
    
            # 7. Po kropce, przed „tłumacz” lub „redakcją” (np. Autor. tłumacz...)
            r'([A-ZŻŹĆĄŚĘŁÓŃ][^.,;\(\)]+?)\.\s*(tłum|redakcja)',
    
            # 8. Autor przed przecinkiem i liczbą stron (poprawiony, aby uwzględnić polskie znaki i różne znaki)
            r'([A-ZŻŹĆĄŚĘŁÓŃ][\w\s\-\.’’]+?),\s*\d+\s*stron[ay]?',
            
            # NOWE wzorce - dopasowanie autora po tytule zakończonym kropką lub przecinkiem
            # 1. Tytuł. Autor. Wydawnictwo, Miasto Rok.
            r'^[^.,\n]+[.,]\s*([A-ZŻŹĆĄŚĘŁÓŃ][\w\s\.\-’\'`]+)\.\s*[A-ZĄĆĘŁŃÓŚŹŻ].*?\d{4}',
            
            # 2. Tytuł, Autor. Wydawnictwo, Miasto Rok.
            r'^[^.,\n]+,\s*([A-ZŻŹĆĄŚĘŁÓŃ][\w\s\.\-’\'`]+)\.\s*[A-ZĄĆĘŁŃÓŚŹŻ].*?\d{4}',
            
            # 3. „Tytuł”, Autor. Wydawnictwo ...
            r'[”"]\s*,\s*([A-ZŻŹĆĄŚĘŁÓŃ][\w\s\.\-’\'`]+)\.\s*[A-ZĄĆĘŁŃÓŚŹŻ].*?\d{4}',
                        
            
        ]
    
        for pattern in patterns:
            try:
                match = re.search(pattern, book_description)
                if match:
                    author_of_book = match.group(1).strip()
                    break
            except:
                continue


        
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|czytanieisluchanie', x)])
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
                             'Opis książki': book_description,
                             'Autor książki': author_of_book,
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


with open(f'data/czytanieisluchanie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/czytanieisluchanie_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    