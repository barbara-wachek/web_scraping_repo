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
#from functions import date_change_format_long

from time import mktime


#%% def    
def get_articles_links(link):
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"}

    html_text = requests.get(link, headers=headers).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    archive_containter_links = [x.a['href'] for x in soup.find('li', class_='widget-container widget_archive').find_all('li')]
    
    articles_links = []
    for link in tqdm(archive_containter_links):
        html_text = requests.get(link, headers=headers).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [x.a['href'] for x in soup.find_all('h2', class_="entry-title")]
        articles_links.extend(links)  

    return articles_links




def dictionary_of_article(article_link):  
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"}
       
    html_text = requests.get(article_link, headers=headers).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date = soup.find('span', class_="entry-date").text.strip()
        date = re.sub(r'(.*\,\s)(\d{1,2}\,?\s)(.*)(\s\d{4}\,?)', r'\2\3\4', date).strip()
        lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12", "styczeń": "01", "luty": "02", "marzec": "03", "kwiecień": "04", "maj": "05", "czerwiec": "06", "lipiec": "07", "sierpień": "08", "wrzesień": "09", "październik": "10", "listopad": "11", "grudzień": "12"}
        for k, v in lookup_table.items():
            date = date.replace(k, v).replace(',', "")
        
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        date_of_publication = format(changed_date.date())
        
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
    try:
        author = soup.find('span', class_="author vcard").find('a').text.strip()
        if author == 'admin':
            author = 'Ewa Bieńczycka'
    except:
        author = None

    try:
        category = " | ".join([x.text.strip() for x in soup.find_all('a', {'rel':'category'})])
    except:
        category = None


    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([e.text for e in article.find_all('p')]).strip()  
    except:
        text_of_article = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'bienczycka|#', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([article_link + x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Kategoria': category,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main

link = 'http://bienczycka.com/blog/'
articles_links = get_articles_links(link)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values(by='Data publikacji')
   

with open(f'data\\bienczycka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\bienczycka_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   