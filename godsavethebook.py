#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_long

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#%% def
def get_archive_links(blog_url):
    html_text = requests.get(blog_url).text
    soup = BeautifulSoup(html_text, 'lxml')
    archive_links = [e['value'] for e in soup.find_all('option') if e['value'] != '']
    return archive_links
    
def get_links_from_archive(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e['href'] for e in soup.find_all('a', class_='continue-reading-link')]
    if soup.find_all('nav', class_='navigation pagination'):
        print('Więcej podstron. Sprawdzić link:' + link)
    articles_links.extend(links)
    
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
    if soup.find('a', class_='url fn n').text.strip() == 'the_book':
        author = "Monika Długa"
    else: 
        author = "Do sprawdzenia"
  
    try:
        title_of_article = soup.find('h1', class_='entry-title singular-title').text.strip()
    except AttributeError:
        title_of_article = None
        
    date_of_publication = soup.find('time', class_='published')['datetime']
    date_of_publication = re.findall(r'\d{4}-\d{2}-\d{2}', date_of_publication)[0]
    categories = " ".join([x.text.strip() for x in soup.find_all('span', class_='bl_categ')])
    article = soup.find('div', class_='entry-content')
    text_of_article = " ".join([e.text for e in article.find_all('p')])
    
    
    if len(text_of_article) < 4 :
        text_of_article = "".join([e.text.strip() for e in article.find_all('div', class_='MsoNormal')])
 
    try:
        title_of_book = re.findall(r'„.*”', title_of_article)[0]
    except IndexError:
        title_of_book = None
        
    try:
        author_of_book = re.findall(r'(?<=” ).*', title_of_article)[0] 
    except IndexError:
        author_of_book = None 
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|godsavethebooks', x)])
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
                             'Kategorie': categories,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': False if external_links == '' else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': photos_links
                             }
            

    all_results.append(dictionary_of_article)


#%% main

archive_links = get_archive_links('https://godsavethebook.pl/')    
   
articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_from_archive, archive_links),total=len(archive_links)))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    
with open(f'data\\godsavethebook_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
      
with pd.ExcelWriter(f"data\\godsavethebook_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



