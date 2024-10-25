#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import time
# from functions import date_change_format_long


from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



#%% def
#Brak dokładnej daty publikacji! z linku można wyciagnac miesiac i rok

def get_articles_links(sitemap_link):
    html_text_sitemap = requests.get(sitemap_link ).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [x.text for x in soup.find_all('loc')]
    articles_links.extend(links)


def dictionary_of_article(article_link): 
    # article_link = 'https://www.malaczcionka.pl/2024/05/kazdy-moze-byc-swiety-czyli-nawet-obuzy.html'
    # article_link = 'https://www.malaczcionka.pl/2023/11/nimona.html'
    # article_link = 'https://www.malaczcionka.pl/2015/12/the-day-crayons-quit.html' #inna struktura artykułów
    #article_link = 'https://www.malaczcionka.pl/2018/04/awantura-o-kapcie.html'
    # article_link = 'https://www.malaczcionka.pl/2016/05/franek-einstein.html'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    author = 'Zosia Gwardyś'
    try:
        title_of_article = soup.find('h1').text.strip()
    except AttributeError:
        title_of_article = None
        
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='post-tags').find_all('a')])
    except: 
        tags = None
    
    try:
        date_of_publication = re.sub(r'(.*)(\d{4})(\/)(\d{2})(.*)', r'\2-\4', article_link)
    except AttributeError:
        date_of_publication = None
    
    try:
        article = soup.find('div', class_='entry-content')
    except AttributeError:
        article = None
        
    try:    
        text_of_article = " ".join([x.text.strip() for x in article.find_all('p')]).strip()
    except AttributeError:
        text_of_article = None
        
    if text_of_article == None or text_of_article == '':
        try: 
            text_of_article = article.text.strip().replace('\n', ' ').replace('  ', ' ')
            # slicing = re.search('\d{2}\:\d{2}', text_of_article).span()[1]
            # text_of_article = text_of_article[slicing:].strip() 
        except:
            text_of_article = None
        
    try:
        book_description = [x.text for x in article.find_all('p') if len(x.text) > 4][0]
    except (AttributeError, KeyError, IndexError, TypeError):
        book_description = None  
        
    if book_description == None or book_description == "":
        try:
            book_description = re.search(r'(?<=\d{2}\:\d{2}).*\d{4}\.?\s\s.', text_of_article).group(0).strip()
        except:
            book_description = None
        
    try:    
        title_of_book = [x for x in article.find_all('p') if len(x.text) > 4][0].b.text
    except (AttributeError, KeyError, IndexError, TypeError):
        title_of_book = None           
    
    if title_of_book == None or title_of_book == "":
        try:
            title_of_book = article.find('b').text
        except:
            title_of_book = None 
    
    try:
        year = re.search(r'\d{4}', book_description).group(0)  
    except (AttributeError, KeyError, IndexError, TypeError):
        year = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'malaczcionka', x)])
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
                             'Tagi': tags,
                             'Opis książki': book_description, 
                             'Tytuł książki': title_of_book,
                             'Rok wydania': year, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        
            
    all_results.append(dictionary_of_article)

#%% main
 
articles_links = []
get_articles_links('https://www.malaczcionka.pl/sitemap.xml')
        

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))



df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)

# df['Opis książki'].isna().sum() #308 pustych opisów #po dodaniu kropki i dwoch spacji: 231 # po wywaleniu spacji i kropki


with open(f'data\\malaczcionka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

with pd.ExcelWriter(f"data\\malaczcionka_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    







#%% #Pozostałości przed odnalezieniem sitemapy

# def get_articles_links(link):   
#     # link = 'https://www.malaczcionka.pl/'
#     options = webdriver.ChromeOptions()
#     #Poniższy wiersz kodu wyłącza wyskakujace okno z wyborem domyślnej przeglądarki!
#     options.add_argument("--disable-search-engine-choice-screen")
#     options.add_argument('--headless')
#     driver = webdriver.Chrome(options=options)
#     driver.get(link)
#     #Czeka na pojawienie się wszystkich elementów grotów, które rozszerzają archwium i uwidaczniają linki do artykułów uszeregowanych wg lat i miesięcy
#     zippy_elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "zippy"))
# )
    
#     #Pętla, która klika w każdy taki grot, aby rozwinąc zawartośc (pobranie linkow pozniej)
#     for element in zippy_elements:
#         try:
#             # Poczekaj, aż element będzie klikalny
#             WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "zippy")))
#             element.click()
#             print('Sukces')
#         except:
#             print(f"Element nie jest interaktywny: {element}")
#                 # Tutaj możesz dodać swoją logikę obsługi błędu, np. przejście do następnego elementu
    
#     #Po tym jak wszystkie zippy_elements są kliknięte, pobranie linkóW: 
#     articles_links = []
#     # all_a_elements = driver.find_elements(By.TAG_NAME, 'a')
#     for a in tqdm(driver.find_elements(By.TAG_NAME, 'a')):
#         href = a.get_attribute('href') 
#         if re.match(r'https:\/\/www\.malaczcionka\.pl\/.*\/\d{2}\/.+', href):
#             if href not in articles_links:
#                 articles_links.append(href)
                
                
    
#     # articles_link_without_duplicates = set(articles_links)
    
#     driver.quit()
#     return articles_links
    
    
    
    
    
    
























