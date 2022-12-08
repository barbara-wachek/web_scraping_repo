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
from functions import date_change_format_short, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def

def get_links_from_sitemap(link):   
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.search(r'https\:\/\/www.zamekczyta.pl\/$', e.text)]
    articles_links.extend(sitemap_links)   
    
def dictionary_of_article(link):
    
    #link = 'https://www.zamekczyta.pl/spoilery-apokalipsy/'
    #link = 'https://www.zamekczyta.pl/monika-glosowitz-czytanie-dobro-luksusowe/'
    link = 'https://www.zamekczyta.pl/kaliningrad-nadprodukcja-znaczen-paulina-siegien/'
    link = 'https://www.zamekczyta.pl/pp-2019-krzysztof-siwczyk-trzy-wiersze/'
    
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    date_of_publication = soup.find('p', class_='single-image-date')
    if date_of_publication:
        new_date = date_change_format_short(date_of_publication.text)
    else:
        new_date = None
        
    
    title_of_article = soup.find('h1')
    if title_of_article:
        title_of_article = soup.find('h1').text.strip()
    else:
        title_of_article = None


    author = soup.find('p', attrs={'style':'text-align: right;'})
    if author:
        author = soup.find('p', attrs={'style':'text-align: right;'}).text.title()
    else:
        author = None
    
    
    content_of_article = soup.find('article')
    
    if content_of_article:
        text_of_article = [x.text for x in content_of_article.find_all('p', class_=None) if x.text != '\xa0']
        if text_of_article:
            text_of_article = " | ".join([x.text.replace('\xa0', '').replace('\n', '') for x in content_of_article.find_all('p', class_=None) if x.text != '\xa0'])
        else:
            text_of_article = None
    else:
        text_of_article = None
        
   
    tags = [x.text for x in content_of_article.find_all('a') if re.search(r'\/tag\/', x['href'])]
    if tags != '':
        tags = " | ".join([x.text for x in content_of_article.find_all('a') if re.search(r'\/tag\/', x['href'])])
    else:
        tags = None


    about_authors = [x.text.replace('\xa0', ' ') for x in content_of_article.find_all('p') if re.match(r'^\p{Lu}{2,}\s[\p{Lu}\s]*(\–|\()', x.text)]
    if about_authors != '':
        about_authors = " | ".join([x.text.replace('\xa0', ' ') for x in content_of_article.find_all('p') if re.match(r'^\p{Lu}{2,}\s[\p{Lu}\s]*(\–|\()', x.text)])
    else:
        about_authors = None
    
    
    book_description = [x.text.replace('\xa0', ' ').strip() for x in content_of_article.find_all('p') if re.match(r'.*\„.*\”.*\d{4}$', x.text)]
    if book_description != '':
        book_description = " | ".join([x.text.replace('\xa0', ' ').strip() for x in content_of_article.find_all('p') if re.match(r'.*\„.*\”.*\d{4}$', x.text)])
    else:
        book_description = None
    
    
    #Autor książki
    #Tytuł książki
    #Rok Wydnia
    #Wydawnictwo
    #tytuły wierszy (np. https://www.zamekczyta.pl/pp-2019-krzysztof-siwczyk-trzy-wiersze/)
    

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'#|zamekczyta', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Nota u autorach': about_authors,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)    
    
    
    
    
    
#%% main

sitemap_post = 'https://www.zamekczyta.pl/post-sitemap.xml'
sitemap_page = 'https://www.zamekczyta.pl/page-sitemap.xml'



articles_links = []
get_links_from_sitemap('https://www.zamekczyta.pl/post-sitemap.xml')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)




















