'''
SELENIUM
'''


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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#%% def    
def get_articles_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    sitemap_publications_links = [x.text for x in soup.find_all('loc') if re.match(r'https:\/\/wpolityce\.pl\/sitemap-publications*', x.text)]
    
    articles_links = []
    for link in tqdm(sitemap_publications_links):
        html_text = requests.get(link).text
        while 'Error 503' in html_text:
            time.sleep(2)
            html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        links = [x.text for x in soup.find_all('loc') if re.match(r'https:\/\/wpolityce\.pl\/kultura\/.+', x.text)]
        articles_links.extend(links)
    
    return articles_links





def dictionary_of_article(driver, article_link): 
    # article_link = 'https://wpolityce.pl/kultura/718751-zmarl-wybitny-rezyser-david-lynch'
    article_link = 'https://wpolityce.pl/kultura/717217-kto-dyrektorem-muzeum-literatury-glinskiplatforma-na-swoim'
    
    driver.get(article_link)
    
    try:
        # Poczekaj na załadowanie najważniejszych elementów
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'article__meta-item js-date-item article__meta-item--shownm'))
        )
        html = driver.page_source
    except Exception as e:
        print(f"Błąd ładowania strony: {e}")
        return None

    soup = BeautifulSoup(html, 'html.parser')
    
    
    
    
    
    
    
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(article_link)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        date = soup.find('li', class_='article__meta-item js-date-item article__meta-item--shown').find('time', class_='').get('title')
        date_of_publication = re.match(r"(\d{4}-\d{2}-\d{2})", date).group(1)
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='article__title').text.strip()
    except:
        title_of_article = None
        
    try:
        author = soup.find('h3', class_="article__author-name").find('a').text.strip()
    except:
        author = None

    try:
        category = soup.find('li', class_='article__division-name').find('a').text.strip()
    except:
        category = None


    article = soup.find('section', class_='article__body')
    
    try:
        text_of_article = " | ".join([e.text for e in article.find_all('p')]).strip()  
    except:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('ul', class_='related-tags__list').find_all('li')])
    except:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'wpolityce|#|tag', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Kategoria': category,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)






























#KOPIA ZAPASOWA:

def dictionary_of_article(article_link): 
    # article_link = 'https://wpolityce.pl/kultura/718751-zmarl-wybitny-rezyser-david-lynch'
    # article_link = 'https://wpolityce.pl/kultura/717217-kto-dyrektorem-muzeum-literatury-glinskiplatforma-na-swoim'
    
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(article_link)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    
    try:
        date = soup.find('li', class_='article__meta-item js-date-item article__meta-item--shown').find('time', class_='').get('title')
        date_of_publication = re.match(r"(\d{4}-\d{2}-\d{2})", date).group(1)
    except:
        date_of_publication = None
    
    try:
        title_of_article = soup.find('h1', class_='article__title').text.strip()
    except:
        title_of_article = None
        
    try:
        author = soup.find('h3', class_="article__author-name").find('a').text.strip()
    except:
        author = None

    try:
        category = soup.find('li', class_='article__division-name').find('a').text.strip()
    except:
        category = None


    article = soup.find('section', class_='article__body')
    
    try:
        text_of_article = " | ".join([e.text for e in article.find_all('p')]).strip()  
    except:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('ul', class_='related-tags__list').find_all('li')])
    except:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'wpolityce|#|tag', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Kategoria': category,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main

sitemap_link = 'https://media.wpolityce.pl/sitemaps/https/index.xml'
articles_links = get_articles_links(sitemap_link)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values(by='Data publikacji')
   

with open(f'data\\hannadikta_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\hannadikta_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   