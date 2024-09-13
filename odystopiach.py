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

#Artykuł pojawia się jako wyskakujacy element. Prawdopodobnie trzeba uzyc selenium

def get_sitemap_links(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    article_link = 'https://odystopiach.blogspot.com/2024/01/inowrocawski-ratusz-z-pegasusem-w-tle.html'
    
    # options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    driver = webdriver.Chrome()
    driver.get(article_link)
    driver.find_element(By.XPATH, "//*[@id='choiceList']/cr-radio-button[3]").click()
    
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "bottom ribbon-piece"))
            )
    finally:
        driver.quit()    
        
        
    #Zrobic tak, zeby poczekało az pokaze sie wybrany element strony
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # author = soup.find('span', class_='post-author vcard').text.replace("\n", " ")
    # author = re.findall(r'(?<=Autor: © ).*', author)[0]
    
    
    try:
        title_of_article = soup.find('h1', class_='title entry-title').text.strip()
    except AttributeError:
        title_of_article = None
        
  
    date_of_publication = soup.find('abbr', class_='time published')
     
    article = soup.find('div', class_='post-body entry-content')
    text_of_article = article.text.strip()
   
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|wcieniuskrzydel', x)])
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
                             'Tekst artykułu': text_of_article,
                             'Inni autorzy':  True if "©" in text_of_article else False,
                             'Linki zewnętrzne': False if external_links == '' else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': photos_links
                             }
            
    all_results.append(dictionary_of_article)

# Pojawiaja sie wiersze wewnatrz tekstow, ktorych autorem jest ktos inny np. tutaj: https://wcieniuskrzydel.blogspot.com/2013/06/dzisiaj-o-2030-wernisaz-online-ksztaty.html


#%% main

articles_links = get_sitemap_links('https://odystopiach.blogspot.com/sitemap.xml')    
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'data\\odystopiach_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
      
with pd.ExcelWriter(f"data\\odystopiach_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



