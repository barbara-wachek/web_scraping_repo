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
from functions import get_links, date_change_format_short


#%% def    
#Wygenerowanie linków do poszczególnych podstron sekcji Kultura
def get_culture_pages(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    last_page = [e.text for e in soup.find('ul', class_='paginatorUl').find_all('li', class_='paginatorLi')][-1].strip()
    format_link = 'https://noizz.pl/kultura?page='
    
    culture_pages = []
    for number in range(int(last_page)+1):
        link = format_link + str(number)
        culture_pages.append(link)
    
    return culture_pages


def get_articles_links(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    articles = [x['href'] for x in soup.find_all('a', class_='itemLink') if re.search(r'^https\:\/\/noizz\.pl\/kultura\/.+', x['href'])] 

    articles_links.extend(articles)


def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    try:
        date_of_publication = soup.find('time')['datetime']
        date_of_publication = re.match(r'^\d{4}-\d{2}-\d{2}', date_of_publication).group()
    except:
        date_of_publication = None
    
    
    try:
        title_of_article = soup.find('h1', class_='title').text.strip()
    except:
        title_of_article = None
        

    
    article = soup.find('div', class_='articleBody')
    
    try:
        lead = soup.find('div', class_='lead').text
        rest_text = " ".join([e.text for e in soup.find('div', class_='whitelistPremium').find_all('p', class_='paragraph')])
    
        text_of_article = lead + rest_text
        
    except:
        text_of_article = None
    
    
    try:
        author = soup.find('span', class_="nameAuthor").text.strip()
        author_split = re.search(r'\n', author).span(0)[0]
        author = author[0:author_split]
    except:
        author = None
    
    
    try:
        tags = " | ".join([x.text for x in soup.find_all('a', class_='itemTagLink')]) 
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'noizz', x)])
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
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
culture_pages = get_culture_pages('https://noizz.pl/kultura')


articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, culture_pages),total=len(culture_pages)))


all_results = []
with ThreadPoolExecutor(max_workers=4) as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')

   
with open(f'data\\noizz_kultura_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\noizz_kultura_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   