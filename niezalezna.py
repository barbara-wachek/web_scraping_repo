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
def get_articles_pages_links(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    articles_pages_links = []
    format_link = 'https://niezalezna.pl/kultura-i-historia?page='
    
    last_page = [e.text for e in soup.find('div', class_='pagenav').find_all('a')][-2]

    for number in range(0, int(last_page)+1):
        link = format_link + str(number)
        articles_pages_links.append(link)

    return articles_pages_links


def get_articles_links(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
   
    links = [x.a['href'] for x in soup.find('div', class_='row cat-desktop').find_all('div', class_='news-title-title') if 'kultura-i-historia' in x.a['href']]
    articles_links.extend(links)
    
    return articles_links




def dictionary_of_article(article_link):  
    # article_link = 'https://niezalezna.pl/kultura-i-historia/nie-zyje-stanislaw-tym-znany-z-kultowych-rol-aktor-zmarl-w-wieku-87-lat/532749'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/91212-pospieszalscy-ruszaja-w-trase-beda-koledowac-w-polsce-francji-austrii-i-na-ukrainie/91212'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/film-o-polakach-ratujacych-zydow-pokazano-na-bialorusi/210873'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/film/where-is-anne-frank-na-cannes-2021-ari-folman-to-dziewczynska-opowiesc-o-dojrzewaniu/403379'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/historia/gdzie-moze-byc-obraz-caravaggia-skradziony-z-palermo/420818'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/ksiazki/laur-niepodleglosci-dla-jacka-sasina-wicepremier-otrzymal-pamiatkowy-album/397783'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/literacki-nobel-zmiany-w-komitecie-noblowskim/246327'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/76357-cos-na-wielki-post/76357'
    # article_link = 'https://niezalezna.pl/kultura-i-historia/102673-alibicom-humor-godny-kac-vegas-recenzja/102673'   #Error 500
    
    response = requests.get(article_link)
    html_text = response.text
    status_code = response.status_code
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    try:
        date_of_publication = soup.find('span', class_='article-date').text
        date_of_publication = re.sub(r'(\d{2})(.)(\d{2})(.)(\d{4})(\s\d{2}:\d{2})', r'\5-\3-\1', date_of_publication)
    except:
        date_of_publication = None
      
             
    try:
        title_of_article = soup.find('h1', class_='tc').text.strip()
    except:
        title_of_article = None
        
        
    try:
        author = soup.find('div', class_="article-author tc mb").find('a').text.strip()
    except:
        author = None

   
    try:
        tags = " | ".join([e.text.replace('#', '') for e in soup.find('p', class_='tags mb mobile-margin').find_all('a')])
    except:
        tags = None


    article = soup.find('div', class_='body')

    try:
       text_of_article = " ".join([e.text for e in article.find_all('p')]).strip()  
    except:
        text_of_article = None
    
   
    try:
        if len(text_of_article) < 1:
            text_of_article = article.text.replace('\n', '').strip()  
    except TypeError:
        text_of_article = None  

    
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'niezalezna|kultura-i-historia|javascript|cdn', x)])
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
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article,
                             'Uwagi': f'{status_code} - błąd serwera' if status_code == 500 else None,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main


articles_pages_links = get_articles_pages_links('https://niezalezna.pl/kultura-i-historia?page=1')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, articles_pages_links),total=len(articles_pages_links)))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


   
with open(f'data\\niezalezna_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\niezalezna_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   