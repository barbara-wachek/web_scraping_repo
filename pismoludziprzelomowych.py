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

def get_article_links(issue):
    
    issue = ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-czerwieclipiec-2016.html', 'czerwiec/lipiec 2016')
    
    issue_link = issue[0]
    issue = issue[1]
        
    html_text = requests.get(issue_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find('div', class_='article hentry ').find_all('a') if re.match(r'^https\:\/\/pismoludziprzelomowych\.blogspot\.com\/p\/\.*', x.text)]
    article_links.extend(links)
        
    return article_links


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
            {'Link' : a.get('href'), 'Numer': issue_name} for a in soup.find('div', class_='article hentry').find_all('a')
            if re.match(r'https?\:\/\/pismoludziprzelomowych\.blogspot\.com\/p\/.*', a.get('href'))
        ]
        
        
        article_links.extend(links)
        
        return links

    finally:
        driver.quit()





#DOKONCZYC

def dictionary_of_article(article_link):  
    
    # article_link = 
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'  # wymuś UTF-8
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    try:
        date_of_publication = soup.find('time', class_='published')['datetime'][:10]
    except:
        date_of_publication = None   
    
    try:
        author = soup.find('span', class_='author-name').text
    except:
        author = None

    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text
    except: 
        title_of_article = None



    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = article.get_text(separator=" ", strip=True).replace('\n', '')
    except:
        text_of_article = None
    
    try:
        category = " | ".join([x.text for x in soup.find('span', class_="cat-links").find_all('a')])
    except:
        category = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='tags-links').find_all('a')])
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'pismoludziprzelomowych', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)
    
    
 
#%% main

issue_links = [('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-czerwieclipiec-2016.html', 'czerwiec/lipiec 2016'), ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-lutymarzec-2016.html', 'luty/marzec 2016'), ('https://pismoludziprzelomowych.blogspot.com/p/poezja-gosc-numeru-tomasz-mielcarek.html', 'listopad/grudzień 2015'), ('https://pismoludziprzelomowych.blogspot.com/p/blog-page_75.html', 'sierpień/wrzesień 2015'), ('https://pismoludziprzelomowych.blogspot.com/p/najnowszy-numer-sierpienwrzesien.html', 'sierpień/wrzesień 2016')]

   
article_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links, issue_links),total=len(issue_links)))




all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/pismoludziprzelomowych_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/pismoludziprzelomowych_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    