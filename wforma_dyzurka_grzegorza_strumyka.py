
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time


#%%def

article_links = []

def get_article_links():     
    for number in range(1, 18):
        format_link = f'https://www.wforma.eu/dyzurka-grzegorza-strumyka,2269,,{number}.html'
        link = format_link 
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        links = ['https://www.wforma.eu/' + e['href'] for e in soup.find('ul', class_='subpagesList').find_all('a')]
        article_links.extend(links)
        
    

def dictionary_of_article(article_link):  

    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')

    try:
        title_of_article = soup.find('h1').get_text(strip=True)
    except:
        title_of_article = None
  
    author = 'Grzegorz Strumyk'
        
    try:
        date_of_publication = soup.find('h6', class_='date').text[:10]
    except:
        date_of_publication = None    
        

    article = soup.find('div', {'id': 'page'})
    
    try:
        text_of_article = article.find('div', class_='content').text.replace('\n', '').replace('\t', ' ')
    except:
        text_of_article = None

        
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'.html', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
                
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img') if 'social_facebook' not in x['src']])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None    

        
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links, 
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main
get_article_links()
 
all_results = []     
with ThreadPoolExecutor(max_workers=10) as executor:
    list(tqdm(executor.map(dictionary_of_article, article_links),
              total=len(all_results)))
       
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)


df.to_json(f'data/wforma_dyzurka_grzegorza_strumyka_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/wforma_dyzurka_grzegorza_strumyka_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   