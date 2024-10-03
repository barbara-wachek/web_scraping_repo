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
from functions import date_change_format_short

#%% def

def number_of_current_issue(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    number = soup.find('a', class_='h2').text.strip()
    number = re.search(r'(?<=Nr\s)\d\d?\d?', number).group(0)
    
    return number


def get_links_of_issues(current_number):
    format_link = 'https://kulturaliberalna.pl/tag/nr-'
    for x in tqdm(range(1, int(current_number)+1)):
        issue_link = format_link + str(x)
        issues_pages_links.append(issue_link)
        
    return issues_pages_links
        

def get_articles_links(issue_link):
    html_text = requests.get(issue_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    issue_links = [x['href'] for x in soup.find_all('a', class_='h5')]
    articles_links.extend(issue_links)

    return articles_links


def dictionary_of_article(article_link):
    # article_link = 'https://kulturaliberalna.pl/2009/01/19/kto-jest-dzis-nazista/'
    # article_link = 'https://kulturaliberalna.pl/2010/05/04/laskowski-slowo-jest-na-koncu-bobby-mcferrin-%e2%80%9evocabularies%e2%80%9d/'
    article_link = 'https://kulturaliberalna.pl/2011/03/22/spiss-zza-sciany/'
    # article_link = 'https://kulturaliberalna.pl/2024/10/01/filip-rudnik-recenzja-litwa-po-litewsku-dominik-wilczewski/'
    # article_link = 'https://kulturaliberalna.pl/2022/04/14/widok-z-k2-dlaczego-propaganda-putina-dziala/'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        author = " | ".join([e.a.text.strip() for e in soup.find_all('div', class_='author')])
    except AttributeError:
        author = None
    try:
         date_and_number = soup.find('article', class_='post-type-post').a.text.strip()
    except AttributeError:
        date_and_number = None
        
    if date_and_number != None:   
        try:   
            date_of_publication = re.findall(r'(?<=\)\s).*', date_and_number)[0]
            date_of_publication = date_change_format_short(date_of_publication)
        except AttributeError:
            date_of_publication = None
        try: 
            number = re.findall(r'^.*\)', date_and_number)[0]
        except AttributeError:
            number = None
    else:
        
        date_of_publication = None
        number = None
   
    try:
        category = soup.find('div', class_='col-18 col-lg-15 col-content').find('div', class_='title').find('h4', recursive=False).text        
    except AttributeError:
        category = None
   

    try:    
        title_of_article = soup.find('h1', class_='post-title').text.strip()
    except AttributeError:
        title_of_article = None        
    
    
    article = soup.find('article', class_='post-type-post')
    
    # article = soup.find('article', class_='post-content')
    # if article == None: 
    #     try:
    #         article = soup.find('div', class_='post-content')
    #     except AttributeError:
    #         article= None
    
    try:
        lead_section = " ".join([x.text.strip() for x in soup.find_all('p', class_='lead')])
    except AttributeError:
        lead_section = NoneType
        
    try:
        text_of_article = " ".join([x.text.strip() for x in soup.find('div', class_='post-content').find_all('p')])
        if lead_section: 
            text_of_article = lead_section +"\n" + text_of_article
    except AttributeError:
        text_of_article = None
        
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'kulturaliberalna', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    if external_links == None or external_links == '':
        try: 
            external_links = ' | '.join([x for x in [x['src'] for x in article.find_all('iframe')] if not re.findall(r'kulturaliberalna', x)])
        except AttributeError:
            external_links = None
         
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Numer': number,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': False if external_links == '' or external_links == None else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': False if photos_links == '' e
                             }
            
    all_results.append(dictionary_of_article)

#%% main

#popraw funkcję dictionary... zeby pokazywalo, jesli artykul ma link do youtube np. ten https://kulturaliberalna.pl/2022/04/14/widok-z-k2-dlaczego-propaganda-putina-dziala/

current_number = number_of_current_issue('https://kulturaliberalna.pl/kategoria/temat-tygodnia/') #821

issues_pages_links = []
get_links_of_issues(current_number)


articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, issues_pages_links),total=len(issues_pages_links)))


#Usunięcie zdublowanych:
articles_links = list(dict.fromkeys(articles_links))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'data\\kulturaliberalna_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
# df['Autor'].notna()

#Jeżeli przy jakimś rekordzie nie udalo sie pobrać daty, to albo link jest już nieaktywny, albo są to jakieś quizy lub materiały promocyjne. Najlepiej wyrzucić na etapie DF, żeby już nikt tego nie analizował niepotrzebnie. 


with pd.ExcelWriter(f"data\\kulturaliberalna_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



