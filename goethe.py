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


    
def get_article_links(link):

    link = 'https://www.goethe.de/ins/pl/pl/kul/lit.html'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept-Language': 'pl-PL,pl;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://przegladdziennikarski.pl/'
    }
    
    response = requests.get(link, headers=headers, allow_redirects=True)
    response.encoding = 'utf-8'
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(link, headers=headers, allow_redirects=True)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    links = [x.get('href') for x in soup.find_all('a') if x.get('href') != None]

    article_links = [x for x in links if re.search(r'\.\.\/\.\.\/pl\/kul\/lit\/\d*\.html', x)]    
  
    
           
    
    return article_links

       

# def dictionary_of_article(article_link):    
    
#     # article_link = 'https://mintmagazine.pl/artykuly/wystawa-niestala-4-x-kolekcja-postapokaliptyczne-spa'
    
#     headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
#                 'Accept-Language': 'pl-PL,pl;q=0.9',
#                 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#                 'Referer': 'https://przegladdziennikarski.pl/'
#             }
#     response = requests.get(article_link, headers=headers, allow_redirects=False)
#     response.encoding = 'utf-8'
    
#     while 'Error 503' in response:
#         time.sleep(2)
#         response = requests.get(article_link, headers=headers, allow_redirects=False)
#     soup = BeautifulSoup(response.text, 'html.parser')
    
    
    
#     try:
#         date = " | ".join([x.text for x in soup.find_all('p', class_='parenthesis') if re.match(r'^\d{2}', x.span.text)])
#         date_of_publication = re.sub(r'(\(\s)(\d{2})(\.)(\d{2})(\.)(\d{4})(\s\))', r'\6-\4-\2', date)
#     except:
#         date_of_publication = None
    
    
#     try:
#         author = soup.find('p', class_="author__name").get_text(strip=True)
#     except:
#         author = None
    

#     try:
#         title_of_article = soup.find('h2', class_='intro__title').text
#     except: 
#         title_of_article = None
        
        
    
#     try:
#         tags = " | ".join([x.get_text(strip=True).replace("(", "").replace(')', '') for x in soup.find('div', class_="share-bar__tags").find_all('span', class_='tag')])
#     except:
#         tags = None
        
    
#     try:
#         text_of_article = " ".join([x.get_text(strip=True) for x in soup.find_all('section', class_='module module--text')])
#     except:
#         text_of_article = None
        
   

#     dictionary_of_article = {'Link': article_link,
#                              'Data publikacji': date_of_publication,
#                              'Autor': author,
#                              'Tytuł artykułu': title_of_article,
#                              'Tekst artykułu': text_of_article,
#                              'Tagi': tags
#                              }
    
#     return dictionary_of_article
 
# #%% main
# article_links = get_article_links('https://www.goethe.de/ins/pl/pl/kul/lit.html')


# all_results = []
# with ThreadPoolExecutor() as executor:
#     all_results = list(
#         tqdm(
#             executor.map(dictionary_of_article, article_links),
#             total=len(article_links)
#         )
#     )

  
# df = pd.DataFrame(all_results).drop_duplicates()
# df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
# df = df.sort_values('Data publikacji', ascending=True)

# with open(f'data/mintmagazine_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
#     json.dump(all_results, f, ensure_ascii=False)    

# with pd.ExcelWriter(f"data/mintmagazine_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
#     df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    