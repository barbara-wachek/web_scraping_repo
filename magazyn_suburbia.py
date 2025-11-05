#%% import 
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
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

#%% def  

def get_issue_links(link):
    # link = 'https://www.magazyn-suburbia.com/archive'
    
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [(e.text.strip(), e.get('href')) for e in soup.find_all('a', {'data-testid':'linkElement'}) if re.match(r'^https\:\/\/www\.magazyn\-suburbia\.com\/archive\/.*', e.get('href'))]
  
    return links

# issue_link = 'https://www.magazyn-suburbia.com/archive/kopia-wydanie-7-9-24-10'

def get_article_links(issue_link):
    #Nie moze byc headless, bo nie wczytuje wszystkich linkow
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(issue_link)
    time.sleep(2)

    # ⬇️ Scrolluj na dół, żeby załadować wszystkie artykuły
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # daj czas na załadowanie
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # ⬇️ Parsuj HTML po scrollowaniu
    soup = BeautifulSoup(driver.page_source, 'lxml')
    driver.quit()

    links = [a['href'] for a in soup.find_all('a', href=True)
             if re.match(r'https://www\.magazyn-suburbia\.com/post/.*', a['href'])]
    
    return list(set(links))






def dictionary_of_article(article):
    
    article_link = article['Link']
    issue = article['Numer']
    
    # article_link = 'https://www.magazyn-suburbia.com/post/miłka-o-malzahn-dziennik-zmian-8'
    # issue = 'Wydanie 6/24(9)'
    
    # # article_link = 'https://www.magazyn-suburbia.com/post/ola-kołodziejek-trzy-wiersze'
    # # issue = 'Wydanie 7-9/24(10)'
    
    # article_link = 'https://www.magazyn-suburbia.com/post/sergio-raimondi-trzy-wiersze'
    
    
    response = requests.get(article_link)

    while 'Error 503' in response.text:
        time.sleep(2)
        response = requests.get(article_link)
    
    # Wymuś poprawne kodowanie
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    
    try:
        title_of_article = soup.find('h1', class_='H3vOVf').text.strip()
    except: 
        title_of_article = None
        

        
    try:
        pattern = r"""
            (
                # 1. Cztery słowa wielką literą (np. Jean Pierre Jeunet Dubois)
                (?:[A-Z][a-ząćęłńóśźżà-ÿ]+\s+){3}[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 2. Trzy słowa wielką literą (np. Gabriel García Márquez)
                (?:[A-Z][a-ząćęłńóśźżà-ÿ]+\s+){2}[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 3. Inicjały z kropkami + nazwisko (np. E.L. James)
                [A-Z]\.[A-Z]\.\s*[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 4. Inicjał + imię + nazwisko (np. F. Scott Fitzgerald)
                [A-Z]\.\s+[A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 5. Imię + inicjał + nazwisko (np. David L. Robbins)
                [A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z]\.\s*[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 6. Podwójne nazwisko (dywiz), np. Ewa Kozyra-Pawlak
                [A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+-[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 7. Nazwisko z dywizem jako imię (rzadziej, ale możliwe): np. Anne-Marie Slaughter
                [A-Z][a-ząćęłńóśźżà-ÿ]+-[A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
        
                | # 8. Imię + nazwisko (np. Stephen King)
                [A-Z][a-ząćęłńóśźżà-ÿ]+\s+[A-Z][a-ząćęłńóśźżà-ÿ]+
            )
        """
        match = re.search(pattern, title_of_article.strip(), re.VERBOSE)
        author = match.group(0) if match else None
        author = author.strip()
    except:
        author = None
        
    try:   
        title_of_masterpiece = " | ".join([x.text.strip() for x in soup.find_all('strong') if x.text.strip() != author])
    except:
        title_of_masterpiece = None
    
    article_div = soup.find('div', class_='hM08C')
    
    if article_div: 
        text_of_article = article_div.text.replace('\n', ' ').replace('\xa0', ' ').replace('  ', ' ').strip()
    else:
        text_of_article = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article_div.find_all('a')] if not re.findall(r'suburbia', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article_div.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Numer czasopisma': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tytuł utworu': title_of_masterpiece,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool(article_div and article_div.find_all('img')),
                             'Filmy': bool(article_div and article_div.find_all('iframe')),
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
    
 
#%% main

# article_links = get_article_links('https://www.magazyn-suburbia.com/blog-posts-sitemap.xml')

issue_links = get_issue_links('https://www.magazyn-suburbia.com/archive')

article_data = []  # tu będą słowniki z linkiem i numerem czasopisma

for issue, link in tqdm(issue_links):
    try:
        links = get_article_links(link)
        for x in links:
            dictionary_of_article = {
                'Link': x,
                'Numer': issue
            }
            article_data.append(dictionary_of_article)
    except Exception as e:
        print(f"Błąd dla {issue} / {link}: {e}")



all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_data),total=len(article_data)))
   

df = pd.DataFrame(all_results).drop_duplicates()


json_data = df.to_dict(orient='records')

with open(f'data/magazynsuburbia_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/magazynsuburbia_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    