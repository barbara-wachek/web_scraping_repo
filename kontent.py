#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.wait import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC



#%% def


def get_issues_links(archive_link):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(archive_link)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    issues_links = [(e.a['href'], e.a.text) for e in soup.find_all('li', class_='jal rm1')]

    return issues_links
    
    
def get_article_links(link):
    
    if link.startswith('http://kontent.net.pl/czytaj'):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        driver.get(link)
        time.sleep(3)
        html = driver.page_source
        driver.quit()
        soup = BeautifulSoup(html, 'html.parser')
        
        links = [e.get('href') for e in soup.find_all('a') if e.get('href') is not None and '/czytaj/' in e.get('href')]
        full_links = [f'https://kontent.net.pl{link}' for link in links]
        articles_links.extend(full_links)

    return articles_links
    
#Problemy z javascriptem. Kontynuować od tego miejsca      
    
def dictionary_of_article(article_link):
    article_link = 'https://kontent.net.pl/czytaj/1#Zestaw_wierszy_M_Prankego'
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(article_link)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    
    
    title_of_article = soup.find('aside').text
    
    author = soup.find('a', class_='url fn').text.strip()
    title_of_article = soup.find('h1', class_='title entry-title').text.strip()
    date_of_publication = soup.find('abbr', class_='time published')['title']
    date_of_publication = re.findall(r'\d{4}-\d{2}-\d{2}', date_of_publication)[0]
    tags = " | ".join([x.text.strip() for x in soup.find_all('a', class_='label')])
    article = soup.find('div', class_='article-content entry-content')
    text_of_article = article.text.strip()
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|chochlikkulturalny', x)])
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
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': False if external_links == '' else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': photos_links
                             }
            

    all_results.append(dictionary_of_article)


#%% main

issues_links = get_issues_links('https://kontent.net.pl/strona/89') #linki z numerami czasopisma
only_links = [t[0] for t in issues_links]


articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links, only_links),total=len(only_links)))












all_results = []
list(tqdm(map(dictionary_of_article, articles_links),total=len(articles_links)))


# all_results = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    
with open(f'data\\chochlikkulturalny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
      
with pd.ExcelWriter(f"data\\chochlikkulturalny_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



