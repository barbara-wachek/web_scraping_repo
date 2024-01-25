#%% import
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import mktime
import json
from functions import date_change_format_long
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#%% def
def web_scraping_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links   


driver = webdriver.Firefox()
driver.get('https://joannaroszak.blogspot.com/2020/06/ile-wazy-patek-sniegu.html')
element = WebDriverWait(driver, 10).until(
   EC.presence_of_element_located((By.CLASS_NAME, "article-content entry-content"))
)
html_text = driver.page_source
soup = BeautifulSoup(html_text, 'lxml')


    
def dictionary_of_article(article_link):
    article_link = articles_links[99]
    article_link = 'https://joannaroszak.blogspot.com/2016/09'
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
     
    
    date_of_publication = soup.find('abbr', class_='time published')['title'][:10]
   
    content_of_article = soup.find('div', class_='article-content entry-content')
    
    tags = None
  
    author = 'Joanna Roszak'

    text_of_article = content_of_article.text.strip().replace('\n', '')
 
    title_of_article = soup.find('h3', class_='post-title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()     
    else:
        title_of_article = None

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(blogspot)|(jpg)', x)])
    except KeyError:
        external_links = None
        
    try:
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
    except KeyError:
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

articles_links = web_scraping_sitemap('https://joannaroszak.blogspot.com/sitemap.xml')


    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))



with open(f'tomasz_bialkowski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

    
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)

    
with pd.ExcelWriter(f"tomasz_bialkowski_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posty', index=False)    
    
    
    
    
    
    
    
    
    
    
    
    
        
        