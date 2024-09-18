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
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



#%% def

def get_sitemap_links(sitemap):
    html_text = requests.get(sitemap).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    return sitemap_links
    
def get_article_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(links)
    
def dictionary_of_article(article_link):
    options = webdriver.ChromeOptions()
    #Poniższy wiersz kodu wyłącza wyskakujace okno z wyborem domyślnej przeglądarki!
    options.add_argument("--disable-search-engine-choice-screen")
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(article_link)
    #Poniższy kod czeka na pojawienie się sekcji o autorstwie (aż wczyta się wyskakujące okno)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'publish-info')))
        
    html_text = driver.page_source 
    soup = BeautifulSoup(html_text, 'html.parser')
    
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

sitemap_links = get_sitemap_links('https://chochlikkulturalny.blogspot.com/sitemap.xml')    
   
articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links, sitemap_links),total=len(sitemap_links)))

#bez wielowątkowości zajmuje około 1,5 godziny. Z wielowątkowoscią wysypuje sie w trakcie
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
   
    



