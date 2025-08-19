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
from functions import date_change_format_short

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By



#%% def    
def get_archive_links(link):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")   # od Chrome 109 zamiast --headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(link)

    # poczekaj chwilę aż select się załaduje
    time.sleep(3)

    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')

    select_part = [x.get('value') for x in soup.find('select', {'id': 'js-wydanie'}).find_all('option')]
    
    archive_links = []
    for id_link in select_part:
        archive_link = 'https://nowynapis.eu/tygodniki/archiwum?k=all&t=' + id_link
        archive_links.append(archive_link)
        
    driver.quit()
    return archive_links
     
    
def get_article_links(archive_link):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")   # od Chrome 109 zamiast --headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(archive_link)

    # poczekaj chwilę aż select się załaduje
    time.sleep(3)

    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')
    
    div_archive = soup.find('div', class_='archiv--boxes')

    for x in div_archive.find_all('div', class_=re.compile("^weekly")):
        a_tag = x.find('a', class_=re.compile("^weekly"))
        if a_tag and a_tag.get("href"):
            article_links.append('https://nowynapis.eu'+ a_tag.get("href"))
            
            

def dictionary_of_article(article_link):    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    
    try:
        author = " | ".join([x.get_text(strip=True) for x in soup.find('div', class_='article--info--row author').find_all('a')])
    except:
        author = None
    
    div_article_info = soup.find('div', class_='article--info')
    other_people = None
    
    try:
        other_people_div = soup.find('div', class_='article--info--authors')
        
        if other_people_div.find('div', class_='article--info--value inline'):
            other_people = " | ".join([other_people_div.find('span', class_='article--info--badge').get_text(strip=True) + " " + x.get_text(strip=True) for x in other_people_div.find('div', class_='article--info--value inline').find_all('a')])
    
    except:
        other_people = None

    
    badges = div_article_info.find_all("span", class_="article--info--badge")
    
    tags = None
    category = None
    date_of_publication = None
    issue = None
    
    
    for badge in badges:
        if "Data publikacji" in badge.text:
            # pobieramy następną div z klasą article--info--value
            value_div = badge.find_next_sibling("div", class_="article--info--value")
            if value_div:
                date = value_div.find("span").text.strip()

        if "Kategoria" in badge.text:
            value_div = badge.find_next_sibling("div", class_="article--info--value")
            if value_div:
                category = " | ".join([x.text.strip() for x in value_div.find_all("a")])
        
        if "Słowa kluczowe:" in badge.text:
            value_div = badge.find_next_sibling("div", class_="article--info--value")
            if value_div:
                tags = " | ".join([x.text.strip() for x in value_div.find_all("a")])
                
        if "Wydawnictwo" in badge.text:
            value_div = badge.find_next_sibling("div", class_="article--info--value")
            if value_div:
                issue_description = " | ".join([x.text.strip() for x in value_div.find_all("a")])
                                 
    try:
        date_of_publication = re.sub(r'(\d{2})(\.)(\d{2})(\.)(\d{4})', r'\5-\3-\1', date)
    except:
        date_of_publication = None
        
        
    try: 
        issue = re.search(r'(#)(\d*)', issue_description).group(2)
    except:
        issue = None
        
    
    try:
        title_of_article = [x.get_text(strip=True) for x in soup.find('h1', class_='small').find_all('span')][1]
    except: 
        title_of_article = None
    
    article = soup.find('div', class_='article--content-box')
    
    try:
        text_of_article = " ".join([x.get_text(strip=True) for x in article.find_all('p')])
    except:
        text_of_article = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'^/.*', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Numer': issue,
                             'Autor': author,
                             'Współtwórcy': other_people,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool(article and article.find_all('img')),
                             'Filmy': bool(article and article.find_all('iframe')),
                             'Linki do zdjęć': photos_links
                             }
    
    all_results.append(dictionary_of_article)
    
    return all_results
 
#%% main

archive_links = get_archive_links('https://nowynapis.eu/tygodniki/archiwum')

article_links = []   
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_article_links, archive_links), total=len(archive_links)))

#usun duplikaty
article_links = list(set(article_links)) 


all_results = []   
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(dictionary_of_article, article_links),total=len(article_links)))

  
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

with open(f'data/nowynapis_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/nowynapis_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    