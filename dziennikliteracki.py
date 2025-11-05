#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
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


    response = requests.get(article_link)
    response.encoding = 'utf-8'  # wymuś UTF-8
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # html_text = requests.get(article_link).text
    # while 'Error 503' in html_text:
    #     time.sleep(2)
    #     html_text = requests.get(article_link).text
    # soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        author = [x.get_text(separator=' ', strip=True).strip() for x in soup.find('div', class_='col-span-12 md:col-span-4 md:col-start-9 lg:col-span-3 lg:col-start-9').find_all('h5')][0]
    except:
        author = None
    
    try:
        all_people = " | ".join([x.get_text(separator=' ', strip=True).strip() for x in soup.find('div', class_='col-span-12 md:col-span-4 md:col-start-9 lg:col-span-3 lg:col-start-9').find_all('h5')])
    except:
        all_people = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title font-bold mb-6').get_text(separator=' ', strip=True).replace('\xa0', ' ')
    except: 
        title_of_article = None
        
    try:
        date = soup.find('div', class_='article-date').text
        date_of_publication = datetime.strptime(date, '%d.%m.%Y')
        date_of_publication = date_of_publication.strftime('%Y-%m-%d')
    except:
        date_of_publication = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='mb-4').find_all('a')])
    except:
        tags = None


    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.text.strip().replace('\xa0', ' ') for x in article.find_all('p')])
    except:
        text_of_article = None
    
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'dziennikliteracki', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Wszystkie osoby': all_people,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('https://dziennikliteracki.pl/post-sitemap.xml')
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/dziennikliteracki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/dziennikliteracki_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    