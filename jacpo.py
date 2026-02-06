
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
    links = [e.text for e in soup.find_all('loc')]
    return article_links.extend(links)




def dictionary_of_article(article_dictionary):

    article_link = article_dictionary['Link']

    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')

    try:
        article_dictionary['Tytuł artykułu'] = soup.find('h1', {'id':'brxe-anifyc'}).get_text(strip=True)
    except:
        article_dictionary['Tytuł artykułu'] = None

    article_dictionary['Autor'] = 'Jacek Podsiadło'

    article = soup.find('div', {'id':'brxe-qqftiy'})

    try:
        article_dictionary['Tekst artykułu'] = " ".join(
            x.text for x in article.find_all('p')
        )
    except:
        article_dictionary['Tekst artykułu'] = None
        
    if article_dictionary['Tekst artykułu'] == None:
        try:
            article_dictionary['Tekst artykułu'] = " ".join(
                x.text for x in soup.find('div', {'id': 'brxe-hicitc'})
            )
        except:
            article_dictionary['Tekst artykułu'] = None
            

    try:
        article_dictionary['Kategoria'] = " | ".join(
            x.text for x in soup.find_all('a', class_='item')
            if 'kategoria' in (x.get('href') or '')
        )
    except:
        article_dictionary['Kategoria'] = None

    try:
        article_dictionary['Tagi'] = " | ".join(
            x.span.text for x in soup.find_all('a', class_="bricks-button") if x.span
        )
    except:
        article_dictionary['Tagi'] = None

    try:
        article_dictionary['Linki zewnętrzne'] = " | ".join(
            href for href in (a.get('href') for a in article.find_all('a'))
            if href and 'jacpo' not in href
        )
    except:
        article_dictionary['Linki zewnętrzne'] = None

    article_dictionary['Zdjęcia/Grafika'] = bool(article and article.find_all('img'))
    article_dictionary['Filmy'] = bool(article and article.find_all('iframe'))

    return article_dictionary




#%%main


sitemap_urls = ['https://jacpo.pl/post-sitemap1.xml', 'https://jacpo.pl/post-sitemap2.xml']
article_links = []

all_results = []


for number in range(1, 28):
    url = f'https://jacpo.pl/strona/{number}/'
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'html.parser')

    for x in soup.find_all('article'):
        a_tag = x.find('a')
        date_div = x.find('div', class_='brxe-gfqmao brxe-text-basic')

        if a_tag and date_div:
            all_results.append({
                'Link': a_tag.get('href'),
                'Data publikacji':  date_formatting(date_div.text.strip())
            })        
        

        
with ThreadPoolExecutor(max_workers=10) as executor:
    list(tqdm(executor.map(dictionary_of_article, all_results),
              total=len(all_results)))
   
    
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)



df.to_json(f'data/jacpo_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/jacpo_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   