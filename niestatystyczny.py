
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
    
    date = date.strip()
    day, rest = date.split(' ', 1)
    month_pl, year = rest.split()
    
    formatted = f"{year}-{months[month_pl]}-{day.zfill(2)}"
    return formatted


def get_article_links(sitemap_url): 
    html_text = requests.get(sitemap_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    links = [e.text for e in soup.find_all('loc')]
    return article_links.extend(links)




def dictionary_of_article(article_link):  
    
    #article_link = 'https://niestatystyczny.pl/2019/02/17/stranger-things-mroczne-umysly-przedpremierowy-fragment-ksiazki/'
    #article_link = 'https://niestatystyczny.pl/2025/12/08/23333/'

    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    
    
    
    try:
        title_of_article = soup.find('h1').text
    except:
        title_of_article = None
  
    try:
        category = " | ".join([x.text for x in soup.find('span', class_='cat').find_all('a')])
    except:
        category = None

    try:
        author = soup.find('div', class_='article-header--info').find('a', class_="nick").text
    except:
        author = None
    
        
    try:
        tags = " | ".join([x.text for x in soup.find_all('a',{'rel':'tag'})])
    except:
        tags = None  
         
        
    try:
        date = soup.find('span', class_='post-date').text[18:]
        date_of_publication = date_formatting(date)
        
    except:
        date_of_publication = None    
        


    article = soup.find('div', class_='post-entry')
    
    try:
        text_of_article = " | ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    
    
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'niestatystyczny', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

        
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False,
                             'Linki do zdjęć': photos_links
                             }

    all_results.append(dictionary_of_article)


#%%main


sitemap_urls = ['https://niestatystyczny.pl/post-sitemap.xml', 'https://niestatystyczny.pl/post-sitemap2.xml', 'https://niestatystyczny.pl/post-sitemap3.xml']
article_links = []
for url in sitemap_urls:
    get_article_links(url)




all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)



df.to_json(f'data/niestatystyczny_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/niestatystyczny_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   