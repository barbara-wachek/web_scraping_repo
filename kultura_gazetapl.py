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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#%% def
def get_category_links(home_page):
    html_text = requests.get(home_page).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('li', class_='nav__item')]
    categories = [e.text.strip() for e in soup.find_all('li', class_='nav__item')]
    dictionary_of_category_links = {categories[e]: links[e] for e in range(len(links))}

    return dictionary_of_category_links
       
#Funkcja rekurencyjna
def get_articles_links(category_link):
    format_link = 'https://kultura.gazeta.pl'
    html_text = requests.get(category_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('article', class_='article')]
    articles_links.extend(links)
    
    try:
        if soup.find('a', class_='next')['href']:
            next_page = format_link + soup.find('a', class_='next')['href']
            return get_articles_links(next_page)
    except TypeError:
        return articles_links
      

def dictionary_of_article(article_link):
    #Rezygnacja z pobierania linkow i zdjec, bo tego jest duzo i nie sa zwiazane z artykulem. Jest od groma reklam i odnosnikow
    # article_link = 'https://kultura.gazeta.pl/kultura/7,127222,31321567,marta-piasecka-nowa-prowadzaca-wiadomosci-w-wpolsce24.html'
    # article_link = 'https://kultura.gazeta.pl/kultura/7,127222,31317522,w-nowych-odcinkach-m-jak-milosc-bedzie-sie-dzialo-do-obsady.html'
    # article_link = 'https://kultura.gazeta.pl/kultura/7,114528,31320808,j-k-rowling-nie-osiada-na-laurach-autorka-harry-ego-pottera.html'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
    author = soup.find('span', class_='article_author').text.strip()
    date_of_publication = soup.find('span', class_='article_date').time['datetime']
    date_of_publication = re.findall(r'\d{4}-\d{2}-\d{2}', date_of_publication)[0]
    title_of_article = soup.find('h1', {'id':'article_title'}).text.strip()
    
    # article_section = [e.text.strip() for e in soup.find_all('div', class_='bottom_section')] #Tu pobierał się cały tekst w tym reklamy
    
    #Do porownania z innymi linkami czy cala zawartosc artykulu jest pobrana. Moze jakies inne elementy pojawia sie w innych przykladach
    
    lead_section = [e.text for e in soup.find_all('div', {'id': 'gazeta_article_lead'})]
    article = [e.text.strip() for e in soup.find_all(re.compile('p|h2|blockquote'), class_= re.compile('art_paragraph|art_sub_title|art_blockquote'))] 
    
    text_of_article = "\n".join(lead_section + article)
    tags = " | ".join([e.text.strip() for e in soup.find_all('li', class_='tags_item')])

    category = soup.find('a', class_='nav__itemName nav__active').text
   
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category,
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article
                             }
            
    all_results.append(dictionary_of_article)

#%% main
# 'https://gazeta.pl/robots.txt' - Zabranianie rozpowszechniania i kopiowania treści. 
#  https://kultura.gazeta.pl/robots.txt


dictionary_of_category_links = get_category_links('https://kultura.gazeta.pl/kultura/0,0.html')    
category_links = [v for k,v in dictionary_of_category_links.items()]


articles_links = []
get_articles_links(category_links[0]) #Wiadomości (około 27 421)
get_articles_links(category_links[1]) #Filmy (około 9 537)
get_articles_links(category_links[2]) #Muzyka (około 5149)
get_articles_links(category_links[3]) #Książki (około 1927)
get_articles_links(category_links[4]) #TV i Seriale (około 7 514)
get_articles_links(category_links[5]) #Sztuka (około 681)
get_articles_links(category_links[6]) #Festiwale (około 175)
get_articles_links(category_links[7]) #Quizy (około 2410)


#Wszystkie artykuły: 54 814
articles_links = list(dict.fromkeys(articles_links))

# articles_links_without_duplicates = list(dict.fromkeys(articles_links))


#Wszystkie artykuły bez duplikatów: 29 701 (niektóre najwyraźniej miały kilka kategorii)

# list(tqdm(map(get_articles_links, category_links),total=len(category_links)))

# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_articles_links, category_links),total=len(category_links)))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'data\\kultura_gazetapl_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   


with pd.ExcelWriter(f"data\\kultura_gazetapl_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



