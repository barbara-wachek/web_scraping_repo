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

def get_article_links(sitemap_links):
    
    if isinstance(sitemap_links, str):  # jeśli to pojedynczy link
        sitemap_links = [sitemap_links]  # zamień na listę

    all_article_links = []
    for sitemap_link in sitemap_links:
        html_text = requests.get(sitemap_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.text for e in soup.find_all('loc') if re.search(r'https\:\/\/wielkibuk\.com\/\d{4}.*', e.text)]
        all_article_links.extend(links)
        
    return all_article_links

def dictionary_of_article(article_link):
    
    # article_link = 'https://wielkibuk.com/2012/12/03/hobbit-czyli-tam-i-z-powrotem-j-r-r-tolkien'
    # article_link = 'https://wielkibuk.com/2012/12/13/piecdziesiat-twarzy-greya-e-l-james/'
    # article_link = 'https://wielkibuk.com/2013/02/10/o-milosci-i-innych-demonach-gabriel-garcia-marquez/'
    # article_link = 'https://wielkibuk.com/2013/11/08/piekielne-wody-david-l-robbins/'
    # article_link = 'https://wielkibuk.com/2014/11/24/male-lisy-justyna-bargielska/'
    
    response = requests.get(article_link)

    while 'Error 503' in response.text:
        time.sleep(2)
        response = requests.get(article_link)
    
    # Wymuś poprawne kodowanie
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    author = 'Olga Kowalska'

    try:
        title_of_article = soup.find('h1', class_='entry-title').text
    except: 
        title_of_article = None
        
        
    try:
        date_of_publication = soup.find('time', class_='entry-date published')['datetime'][:10]
    except:
        date_of_publication = None

    try:
        category = " | ".join([x.text for x in soup.find('span', class_='cat-links').find_all('a')])
    except:
        category = None


    try:
        title_of_book = " | ".join(re.findall(r'[„"](.*?)["”]', title_of_article))
    except:
        title_of_book = None
        
    try:
        pattern = r"""
            (
                # 1. Cztery słowa wielką literą (np. Jean Pierre Jeunet Dubois)
                (?:[A-Z][a-ząćęłńóśźżà-ÿ]+\s+){3}[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 2. Trzy słowa wielką literą (np. Gabriel García Márquez)
                (?:[A-Z][a-ząćęłńóśźżà-ÿ]+\s+){2}[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 3. Nazwisko z łącznikiem (np. Anne-Marie Slaughter)
                [A-Z][a-ząćęłńóśźżà-ÿ]+-[A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 4. Inicjały z kropkami + nazwisko (np. E.L. James)
                [A-Z]\.[A-Z]\.\s*[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 5. Inicjał + imię + nazwisko (np. F. Scott Fitzgerald)
                [A-Z]\.\s+[A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 6. Imię + inicjał + nazwisko (np. David L. Robbins)
                [A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z]\.\s*[A-Z][a-ząćęłńóśźżà-ÿ]+
                |
                # 7. Imię + nazwisko (np. Stephen King)
                [A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
            )$
        """
        match = re.search(pattern, title_of_article.strip(), re.VERBOSE)
        author_of_book = match.group(0) if match else None
    except:
        author_of_book = None

    
    article = soup.find('div', class_='entry-content')
    
    if article: 
        text_of_article = " ".join([x.text for x in article.find_all('p')]).replace('\n', ' ').replace('  ', ' ').strip()
    else:
        text_of_article = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'wielkibuk', x)])
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
                             'Kategoria': category,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x.get('src') for x in article.find_all('img')] else False,
                             'Filmy': True if [x.get('src') for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
all_article_links = get_article_links(['https://wielkibuk.com/sitemap-1.xml', 'https://wielkibuk.com/sitemap-2.xml'])


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_article_links),total=len(all_article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/wielkibuk_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/wielkibuk_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     
   
    


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    