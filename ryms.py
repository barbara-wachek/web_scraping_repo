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


#%% def
def get_all_pages_links(category_link):
    format_link = 'page/'
    html_text = requests.get(category_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    try:
        last_page= soup.find('div', class_='page-nav td-pb-padding-side').find('a', class_='last').text
    except AttributeError:
        last_page = re.search(r'\d*$', soup.find('span', class_='pages').text).group(0)
    
    for x in range(1, int(last_page)+1):
        page_link = category_link+format_link+str(x)
        all_pages_links.append(page_link)
        
    return all_pages_links
        

def get_articles_links_from_category_pages(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    block_with_articles = soup.find('div', class_='td-pb-span8 td-main-content')
    links = [x.a['href'] for x in  block_with_articles.find_all('h3', class_='entry-title td-module-title')]
    articles_links.extend(links)
    
    return articles_links



#Problemy: jak wyciągnąć imię i nazwisko autora. Często nie ma tego przy artykule. Czasami jest w tekcie na koncu. Nie wyciągam wcale, bo za duzo jest wtedy bledów. 
# Wyciągnij tytułu ksiażek z kategorii recenzja
# Czy ta strona nie powinna isc do opracowania manualnego? 



def dictionary_of_article(article_link):
    # article_link = 'https://ryms.pl/kowboj-prawde-ci-powie/'
    # article_link = 'https://ryms.pl/hola-hola-literatura/'
    # article_link = 'https://ryms.pl/10-edycja-festiwalu-literatury-dla-dzieci/'
    # article_link = 'https://ryms.pl/nominacje-do-nagrody-conrada/' #autor na koncu w nawiacsie (ml)
    # article_link = 'https://ryms.pl/o-czechu-ktory-pokochal-wlochy/'
    # article_link = 'https://ryms.pl/wielka-podroz-dziadka-eustachego/'
    # article_link = 'https://ryms.pl/zlotowlosa-i-trzy-niedzwiedzie/'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
  
    try:
        date_of_publication = re.search(r'.*(?=T)', soup.find('time', class_='entry-date updated td-module-date')['datetime']).group(0)
    except (AttributeError, TypeError):
        date_of_publication = None
        
        
    try:
        category = " | ".join([x.text for x in soup.find_all('li', class_='entry-category')])
    except AttributeError:
        category = None
   

    try:    
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except AttributeError:
        title_of_article = None        
    
    
    article = soup.find('div', class_='td-post-content tagdiv-type')
        
    try:
        text_of_article = " ".join([x.text.strip() for x in article.find_all('p')])
    except AttributeError:
        text_of_article = None
        
    # try:
    #     author = " | ".join(re.findall(r'Marta Lipczyńska-Gil|Paweł Pawlak|Nika Jaworowska-Duchlińska|Marianna Sztyma|Maria Dek-Lewandowska|Juszczak Agata|Kożuchowska Agnieszka|Agnieszka Wolny-Hamkało|Lengren Zbigniew|Flisak Jerzy|Ambrożewski Jacek|Wilkoń Józef', text_of_article))
    # except (AttributeError, TypeError):
    #     author = None
    
    # if author == []:
    #     author = None
    
    try:
        title_of_book = re.search(r'^„.*”', title_of_article).group(0)
    except (AttributeError, TypeError):
        title_of_book = None
        
    if title_of_book == None:
        try:
            title_of_book = re.findall(r'„(.*?)”', text_of_article)[0]
        except (TypeError, IndexError):
            title_of_book = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('ul', class_='td-tags td-post-small-box clearfix').find_all('a')])
    except AttributeError:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'ryms', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None        

         
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             # 'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category,
                             'Tagi': tags,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': False if external_links == '' or external_links == None else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': False if photos_links == '' else photos_links
                             }
            
    all_results.append(dictionary_of_article)

#%% main


category_links = ['https://ryms.pl/category/recenzje/', 'https://ryms.pl/category/autorzy/', 'https://ryms.pl/category/wydarzenia1/', 'https://ryms.pl/category/artykuly/']


all_pages_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_all_pages_links, category_links),total=len(category_links)))



articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links_from_category_pages, all_pages_links),total=len(all_pages_links)))
    
    
    
#1611 artykułów
#Usunięcie zdublowanych:
articles_links = list(dict.fromkeys(articles_links)) #1607 bez zdublowanych



all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
df = df.dropna(subset=["Data publikacji"]) #Usunięcie dwóch linków bez daty (podstrony)


with open(f'data\\ryms_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

with pd.ExcelWriter(f"data\\ryms_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



