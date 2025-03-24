#%% import 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urljoin
import json


#%% def    
#Decyzja PCzL, że pobieramy tylko Artykuły i wywiady
'https://biuletynpolonistyczny.pl/pl/articles/'

articles_links = []
def get_articles_links(link): 
    # link = 'https://biuletynpolonistyczny.pl/pl/articles/?&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf&article_contributors=&o=-article_date_add&article_title_text=&page=1&per_page=90'
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    last_page = soup.find('li', class_='paginate__item paginate__item--pages').find('a', class_='paginate__pages').text
    
    
    for number_of_page in range(int(last_page) + 1):
        link = f'https://biuletynpolonistyczny.pl/pl/articles/?&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf&article_contributors=&o=-article_date_add&article_title_text=&page={number_of_page}&per_page=90'
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        links = ['https://biuletynpolonistyczny.pl' + x.get('href') for x in soup.find('div', class_='list__box--listing list__box--listing-upperline').find_all('a')]
        articles_links.extend(links)

    return articles_links



    
#News / AKtualnosci    
    
# def get_articles_links(link): 
#     link = 'https://biuletynpolonistyczny.pl/pl/news/?&o=-new_date_add&page=1&per_page=90'
#     # link = 'https://biuletynpolonistyczny.pl/pl/articles/?&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf&article_contributors=&o=-article_date_add&article_title_text=&page=1&per_page=90'
#     html_text_sitemap = requests.get(link).text
#     soup = BeautifulSoup(html_text_sitemap, 'lxml')
#     last_page = soup.find('li', class_='paginate__item paginate__item--pages').find('a', class_='paginate__pages').text
    
    
#     for number_of_page in range(int(last_page) + 1):
#         link = f'https://biuletynpolonistyczny.pl/pl/news/?&o=-new_date_add&page={number_of_page}&per_page=90'
#         html_text_sitemap = requests.get(link).text
#         soup = BeautifulSoup(html_text_sitemap, 'lxml')
#         links = ['https://biuletynpolonistyczny.pl' + x.get('href') for x in soup.find('div', class_='list__box--listing list__box--listing-upperline').find_all('a')]
#         articles_links.extend(links)

#     return articles_links

# articles_links_drop_duplicates = list(set(articles_links))   
     
    

    
def dictionary_of_article(article_link):
    # article_link = 'https://biuletynpolonistyczny.pl/pl/articles/nowele-polskie-lektura-narodowego-czytania-2019,42/details?page=1&per_page=90&article_contributors=&o=-article_date_add&page=0&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf&article_title_text=&per_page=90'
    # article_link = 'https://biuletynpolonistyczny.pl/pl/articles/losy-absolwentow-studiow-polonistycznych-przyklad-zielonogorski-wyniki-badania-ankietowego,14/details?page=1&per_page=90&article_contributors=&o=-article_date_add&page=3&per_page=90&article_title_text=&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    try:
        category = soup.find('h3', class_='details__title--head').text
    except:
        category = None   
        
    try:   
        date = soup.find('span', class_='details__text--date').text
        date_of_publication = re.sub(r'(\d{2})(\.)(\d{2})(\.)(\d{4})', r'\5-\3-\1', date)
    except:
        date_of_publication = None 
             
    try:
        title_of_article = soup.find('h1', class_='details__text--title').text.strip()
    except:
        title_of_article = None
   
    
    article = soup.find('div', class_='details__box--content')
    
    
    try:
        rows = soup.find_all("div", class_="details__information--row")
        tags = []

        for row in rows:
            label = row.find("span", class_="details__text--label")
            if label and "Słowa kluczowe" in label.text:
                tags = " | ".join([a.text.strip() for a in row.find_all("a", class_="details__text--anchor-border")])
                break  
    except:
        tags = None
    
    try:
        authors = []
        for row in soup.find_all("div", class_="details__information--row"):
            label = row.find("span", class_="details__text--label")
            if label and "Autor" in label.text:
                value = row.find("a", class_="details__text--value")
                if value:
                    authors.append(value.text.strip())
        
        author = " | ".join(authors)
    except: 
        author = None
    
    
    if author == None or author == '':
        try: 
            rows = soup.find_all("div", class_="details__information--row")

            for row in rows:
                label = row.find("span", class_="details__text--label")
                if label and ("Data dodania" in label.text or "Data edycji" in label.text):
                    value = row.find("span", class_="details__text--value")
                    if value:
                        match = re.search(r"\((.*?)\)", value.text)  # Szuka tekstu w nawiasach
                        if match:
                            author = match.group(1)
                            break  # Po znalezieniu autora kończymy
        except:
            author = None
                
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'^/pl/|^/|^#', x)])
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
                             'Kategoria': category,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Filmy': True if article and article.find_all('iframe') else False,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
articles_links = []
articles_links = get_articles_links('https://biuletynpolonistyczny.pl/pl/articles/?&csrfmiddlewaretoken=MUNHWmlaHs6O6Y3kapRp4LRnOjfHYflf&article_contributors=&o=-article_date_add&article_title_text=&page=1&per_page=90')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/biuletynpolonistyczny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data/biuletynpolonistyczny_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   



   
   
   
   
   
   
   
   
   
   