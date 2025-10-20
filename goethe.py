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

#trudno pozyskać linki. Dziwna struktura strony, mocno zagnieżdżona

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

def get_article_links(link):
    resp = requests.get(link, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(link, href)
        if "goethe.de/ins/pl/pl/kul/lit/2" in full_url:
            links.add(full_url.split("#")[0])
    
    links = sorted(links)
    return links


    
def dictionary_of_article(article_link):    
    
    # article_link = 'https://www.goethe.de/ins/pl/pl/kul/lit/21225167.html'
    # article_link = 'https://www.goethe.de/ins/pl/pl/kul/lit/22773331.html'
    
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
                'Accept-Language': 'pl-PL,pl;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': 'https://przegladdziennikarski.pl/'
            }
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    try:
        top = soup.find('h1', class_='kultur-artikel-hdl m-lr-a Mt(40px) Mb(16px) w(719px)')
    except:
        top = None
        
    if top != None: 
        try: 
            title_of_article = top.find_all('span')[1].text
        except:
            title_of_article = None
        
        try: 
            tags = top.find_all('span')[0].text
        except:
            tags = None
    else:
        title_of_article = None
        tags = None
    
    
    try:
        about_author = soup.find('div', class_="box author").get_text(strip=True)
    except:
        about_author = None
    
        

    article = soup.find('article', class_='ganze-breite kultur-artikel kultur-artikel-c')   
        
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
        
   

    dictionary_of_article = {'Link': article_link,
                             'Autor': about_author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags
                             }
    
    all_results.append(dictionary_of_article)
    


 
#%% main
article_links = get_article_links("https://www.goethe.de/ins/pl/pl/kul/lit.html")


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))

  
df = pd.DataFrame(all_results).drop_duplicates()

with open(f'data/goethe_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/goethe_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    