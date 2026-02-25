
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time
import random


#%%def

def date_formatting(date): 
    months = {
    'stycznia': '01',
    'lutego': '02',
    'marca': '03',
    'kwietnia': '04',
    'maja': '05',
    'czerwca': '06',
    'lipca': '07',
    'sierpnia': '08',
    'września': '09',
    'października': '10',
    'listopada': '11',
    'grudnia': '12'
    }
    
    day, rest = date.split(' ', 1)
    month_pl, year = rest.replace(',', '').split()
    
    formatted = f"{year}-{months[month_pl]}-{day.zfill(2)}"
    return formatted


def get_article_links(sitemap_url): 
    html_text = requests.get(sitemap_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    links = [e.text for e in soup.find_all('loc') if re.match(r'https\:\/\/stare-kino\.pl\/.+', e.text)]
    return links



def dictionary_of_article(article_link, retries=3, timeout=10):  
    
    #article_link = 'https://marcincielecki.wordpress.com/2020/08/15/andrzej-chwalba-przegrane-zwyciestwo-wojna-polsko-bolszewicka-1918-1920/'
    #article_link = 'https://stare-kino.pl/klasyka-polskiego-kina-w-lyonie-ostatni-etap-i-zew-morza/'
    #article_link = 'https://stare-kino.pl/renata-radojewska-swieza-twarz-polskiego-filmu/' #z autorem i data po lewej
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

    for attempt in range(retries):
        try:
            response = requests.get(article_link, timeout=timeout, headers=headers)
            response.raise_for_status()  # zgłosi błąd, jeśli status != 200
            html_text = response.text
            break  # jeśli wszystko OK, wychodzimy z pętli
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Błąd połączenia: {e}. Próba {attempt + 1} z {retries}")
            time.sleep(2)  # czekamy chwilę przed kolejną próbą
    else:
        print(f"Nie udało się pobrać artykułu: {article_link}")
        return None  # kończymy funkcję, jeśli nie udało się połączyć

    soup = BeautifulSoup(html_text, 'html.parser')

    
    try:
        title_of_article = soup.find('h2', class_='entry-title').get_text(strip=True)
    except:
        title_of_article = None
  


    article = soup.find('article')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None        
        
    try:
        author = article.find('p', {'style':'text-align: right;'}).text
        author = re.search(r'^([^\d]+?)\s*(?=\d)', author).group(1)
    except:
        author = None
            
        
    try:
        date = article.find('p', {'style':'text-align: right;'}).text
        date_of_publication = re.search(r'(\d{1,2}\s+[a-ząćęłńóśźż]+\s+\d{4})', date).group(1)
        date_of_publication = date_formatting(date_of_publication)
    except:
        date_of_publication = None    
    
    
    if date_of_publication == None: 
        script = soup.find("script", type="application/ld+json")
        data = json.loads(script.string)

        for item in data["@graph"]:
            if item.get("@type") == "Article":
                date_of_publication = item.get("datePublished")[:10]

    try:
        categories = " | ".join([x.text for x in soup.find('ul', class_='post-categories').find_all('a')])
    except:
        categories = None
   
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'stare-kino', x) and '#_edn' not in x])
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
                             'Kategorie': categories, 
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links, 
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main
article_links = get_article_links('https://stare-kino.pl/post-sitemap.xml')
 
# all_results = []     
# with ThreadPoolExecutor(max_workers=2) as executor:
#     list(tqdm(executor.map(dictionary_of_article, article_links),
#               total=len(all_results)))


all_results = []

for link in tqdm(article_links):
    dictionary_of_article(link)
    time.sleep(random.uniform(1, 2))  


   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)


df.to_json(f'data/stare-kino_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/stare-kino_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   