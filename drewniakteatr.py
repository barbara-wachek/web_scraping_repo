#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json

#%% def    

def get_article_links(sitemap_link):
    response = requests.get(sitemap_link)
    soup = BeautifulSoup(response.text, 'xml')

    links_with_dates = []
    for url in soup.find_all('url'):
        loc = url.find('loc').text if url.find('loc') else None
        lastmod = url.find('lastmod').text if url.find('lastmod') else None
        if loc:
            links_with_dates.append((loc, lastmod))

    return links_with_dates
    


def dictionary_of_article(article_link, last_mod_date):
    response = requests.get(article_link)
    response.encoding = 'utf-8'  # wymuś UTF-8
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # html_text = requests.get(article_link).text
    # while 'Error 503' in html_text:
    #     time.sleep(2)
    #     html_text = requests.get(article_link).text
    # soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        author = " | ".join([x.get_text(separator=' ', strip=True).strip() for x in soup.find('span', class_='fn').find_all('a')])
    except:
        author = None
    
    try:
        category = " | ".join([x.text for x in soup.find('div', class_='meta-item meta-category').find_all('a')])
    except:
        category = None
    
    try:
        title_of_article = soup.find('h1', class_='entry-title entry-title-cover-empty').get_text(separator=' ', strip=True).replace('\xa0', ' ')
    except: 
        title_of_article = None

    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = " ".join([x.text.strip().replace('\xa0', ' ') for x in article.find_all('p')])
    except:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='entry-tags').find_all('a')])
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'drewniakteatr', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': last_mod_date[:10],
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Kategoria': category, 
                             'Tags': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('https://www.drewniakteatr.pl/post-sitemap.xml')
   
all_results = []
for link, date in article_links:
    dictionary_of_article(link, date)

    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/drewniakteatr_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/drewniakteatr_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    