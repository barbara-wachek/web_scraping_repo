#%% import 
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
from functions import date_change_format_short


#%% def    
def get_articles_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    articles_links = [x.text for x in soup.find_all('loc') if re.match(r'https:\/\/gdybymbylaktorem\.pl\/.+', x.text)]

    return articles_links



def dictionary_of_article(article_link):  
    # article_link = 'https://gdybymbylaktorem.pl/aktorstwo-sztuka-ktora-przekracza-granice/' #test link
    
    response = requests.get(article_link)
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date = soup.find('span', class_='meta-item meta-date').find('span').text
        date_sub = re.sub(r'(\d{1,2})(\s.*)(\,)(\s\d{4})', r'\1\2\4', date).strip()
        lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
        for k, v in lookup_table.items():
            date = date_sub.replace(k, v)
    
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        date_of_publication = format(changed_date.date())
    
    except:
        date_of_publication = None
             
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
    try:
        author = soup.find('div', class_="written-by").find('a').text.strip()
    except:
        author = None

   
    try:
        category = " | ".join([e.text for e in soup.find('div', class_='entry-category').find_all('a')])
    except:
        category = None


    article = soup.find('div', class_='johannes-section')

    try:
        text_of_article = " ".join([e.text for e in soup.find('div', class_='entry-content entry-single clearfix').find_all('p')]).strip()  
    except:
        text_of_article = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'gdybymbylaktorem', x)])
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
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
articles_links = get_articles_links('https://gdybymbylaktorem.pl/post-sitemap.xml')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)
df = df.drop_duplicates()

   
with open(f'data\\gdybymbylaktorem_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\gdybymbylaktorem_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   
