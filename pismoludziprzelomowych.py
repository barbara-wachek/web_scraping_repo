#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

#%% def    

# def get_article_links(issue):
    
#     # issue = ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-czerwieclipiec-2016.html', 'czerwiec/lipiec 2016')
    
#     issue_link = issue[0]
#     issue = issue[1]
        
#     html_text = requests.get(issue_link).text
#     soup = BeautifulSoup(html_text, 'lxml')
#     links = [x.text for x in soup.find('div', class_='article hentry ').find_all('a') if re.match(r'^https\:\/\/pismoludziprzelomowych\.blogspot\.com\/p\/\.*', x.text)]
#     article_links.extend(links)
        
#     return article_links


def get_article_links(issue):
    # issue = ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-czerwieclipiec-2016.html', 'czerwiec/lipiec 2016')
    issue_link, issue_name = issue


    options = Options()
    options.add_argument("--headless")  # usuń, jeśli chcesz widzieć przeglądarkę
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(issue_link)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'body > div.viewitem-panel > div > div.viewitem-inner > div > div > div.article-header > h1'))
        )

        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        links = [
            (a.get('href'), issue_name) for a in soup.find('div', class_='article hentry').find_all('a')
            if re.match(r'https?\:\/\/pismoludziprzelomowych\.blogspot\.com\/p\/.*', a.get('href'))
        ]
        
        
        article_links.extend(links)
        
        return links

    finally:
        driver.quit()





#DOKONCZYC

def dictionary_of_article(article):  
    
    article_link, issue = article
    
    # article_link = 'http://pismoludziprzelomowych.blogspot.com/p/blog-page_56.html'
    # article_link = 'http://pismoludziprzelomowych.blogspot.com/p/rafa-rozewicz_25.html'
    # article_link = 'http://pismoludziprzelomowych.blogspot.com/p/proza-zycia-maja-stasko.html'
    
    options = Options()
    options.add_argument("--headless")  # usuń, jeśli chcesz widzieć przeglądarkę
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)


    driver.get(article_link)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'body > div.viewitem-panel > div > div.viewitem-inner > div > div > div.article-header > h1'))
    )

    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')
    
    try:
        title_of_article = soup.find('h1', class_='title entry-title').get_text(strip=True)
    except:
        title_of_article = None
  
    try:
        author = re.search(r'(.*\: )(.*)', title_of_article).group(2)
    except:
        author = None

    article = soup.find('div', class_='article-content entry-content')
    
    try:
        text_of_article = article.get_text(separator=" ", strip=True).replace('\n', '')
    except:
        text_of_article = None
    
    # try:
    #     title_of_piece = [x.text for x in article.find_all('span')]
    # except:
    #     title_of_piece = None
        
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'pismoludziprzelomowych', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        

        
    dictionary_of_article = {'Link': article_link,
                             'Numer': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)
    driver.quit()


        
 
 
#%% main

issue_links = [('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-czerwieclipiec-2016.html', 'czerwiec/lipiec 2016'), ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-lutymarzec-2016.html', 'luty/marzec 2016'), ('https://pismoludziprzelomowych.blogspot.com/p/poezja-gosc-numeru-tomasz-mielcarek.html', 'listopad/grudzień 2015'), ('https://pismoludziprzelomowych.blogspot.com/p/blog-page_75.html', 'sierpień/wrzesień 2015'), ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-sierpienwrzesien.html', 'sierpień/wrzesień 2016')]

   
article_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links, issue_links),total=len(issue_links)))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()
df_test = df.copy()


#dodanie kolumny forma/gatunek na podstawie Tytuł artykułu

def przypisz_forme(tytul):
    if 'wiersz' in tytul.lower():
        return 'wiersz'
    elif 'proza' in tytul.lower():
        return 'proza'
    else:
        return None

# Tworzenie nowej kolumny
df_test['forma/gatunek'] = df_test['Tytuł artykułu'].apply(przypisz_forme)


df_test.to_json(f'data/pismoludziprzelomowych_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/pismoludziprzelomowych_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df_test.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    