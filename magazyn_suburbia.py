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
    link = 'https://www.magazyn-suburbia.com/archive'
    
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [(e.text.strip(), e.get('href')) for e in soup.find_all('a', {'data-testid':'linkElement'}) if re.match(r'^https\:\/\/www\.magazyn\-suburbia\.com\/archive\/.*', e.get('href'))]
  
    return links



def get_article_links(issue_link):
    # Konfiguracja przeglądarki
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(issue_link)

    # Scrollowanie w dół, żeby załadować wszystkie elementy
    scroll_pause_time = 2
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Przewiń na sam dół
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Czekaj na załadowanie nowych elementów
        time.sleep(scroll_pause_time)

        # Sprawdź nową wysokość strony
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break  # Koniec scrollowania
        last_height = new_height

    # Poczekaj, aż pojawi się ostatni artykuł (opcjonalne, bardziej niezawodne)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='/post/']"))
        )
    except Exception as e:
        print("Timeout while waiting for articles to load:", e)

    soup = BeautifulSoup(driver.page_source, "lxml")
    driver.quit()

    links = [
        a.get("href") for a in soup.find_all("a")
        if a.get("href") and re.match(r'https://www\.magazyn-suburbia\.com/post/.*', a.get("href"))
    ]

    return list(set(links))  # usunięcie duplikatów, jeśli są

# Przykład użycia:
issue_url = "https://www.magazyn-suburbia.com/archive/kopia-wydanie-3-25-16"
lista = get_article_links(issue_url)






















def get_article_links(issue_link, issue):
    issue = 'Wydanie 4/25 (17)'
    issue_link = 'https://www.magazyn-suburbia.com/archive/kopia-wydanie-3-25-16'
    html_text = requests.get(issue_link).text
    
    
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.get('href') for e in soup.find_all('a') if re.match(r'https\:\/\/www\.magazyn\-suburbia\.com\/post\/.*', e.get('href'))]
    return links




def dictionary_of_article(article_link):
    
    # article_link = 'https://www.magazyn-suburbia.com/post/wojciech-brzoska-cztery-wiersze'
    # article_link = 'https://www.magazyn-suburbia.com/post/pierre-vinclair-wiersz'
    # article_link = 'https://www.magazyn-suburbia.com/post/marcin-mielcarek-skoczek-fragment'
    # article_link = 'https://www.magazyn-suburbia.com/post/mary-noonan-cztery-wiersze'
    article_link = 'https://www.magazyn-suburbia.com/post/ewa-kłobuch-pięć-wierszy'

    response = requests.get(article_link)

    while 'Error 503' in response.text:
        time.sleep(2)
        response = requests.get(article_link)
    
    # Wymuś poprawne kodowanie
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    
    author = 'Maja Kupiszewska'

    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    except: 
        title_of_article = None
        
        
    try:
        date_of_publication = soup.find('time', class_='published')['datetime'][:10]
    except:
        date_of_publication = None

    try:
        category = " | ".join([x.text for x in soup.find('div', class_='post-sidebar-item post-sidebar-labels').find_all('a')])
    except:
        category = None
        
        
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='byline post-labels').find_all('a')])
    except:
        tags = None


        
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
        author_of_book = match.group(0) if match else None
    except:
        author_of_book = None
        
        
    try:
        title_of_book = " | ".join(re.findall(r'[„"](.*?)["”]', title_of_article))
    except:
        title_of_book = None
        
    if title_of_book == None or title_of_book == '': 
        try:
            pattern = rf"^(.*?),\s*{re.escape(author_of_book)}(?:,.*)?$"
            match = re.match(pattern, title_of_article)
            if match:
                title_of_book = match.group(1)
        except:
            title_of_book = None

    
    article = soup.find('div', class_='post-body-container')
    
    if article: 
        text_of_article = article.text.replace('\n', ' ').replace('\xa0', ' ').replace('  ', ' ').strip()
    else:
        text_of_article = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'wielkibuk', x)])
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
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category,
                             'Tags': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x.get('src') for x in article.find_all('img')] else False,
                             'Filmy': True if [x.get('src') for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
    
 
#%% main

article_links = get_article_links('https://www.magazyn-suburbia.com/blog-posts-sitemap.xml')

issue_links = get_issue_links('https://www.magazyn-suburbia.com/archive')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_article_links),total=len(all_article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Numer', ascending=True)

json_data = df.to_dict(orient='records')

with open(f'data/tlenliteracki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/tlenliteracki_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    