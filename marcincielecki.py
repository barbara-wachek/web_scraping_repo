
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
    'Maj': '05',
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
    links = [e.text for e in soup.find_all('loc') if re.match(r'https\:\/\/marcincielecki\.wordpress\.com\/\d{4}\/\d{2}\/\d{2}.*', e.text)]
    return links



def dictionary_of_article(article_link):  
    
    #article_link = 'https://marcincielecki.wordpress.com/2020/08/15/andrzej-chwalba-przegrane-zwyciestwo-wojna-polsko-bolszewicka-1918-1920/'
    #article_link = 'https://marcincielecki.wordpress.com/2020/05/21/maciej-j-nowak-narutowicz-niewiadomski-biografie-rownolegle/'

    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'html.parser')

    
    try:
        title_of_article = soup.find('h1', class_='entry-title').get_text(strip=True)
    except:
        title_of_article = None
  
    try:
        author = soup.find('a', class_='url fn n').text
        
        if author == 'mcielecki1':
            author = 'Marcin Cielecki'
    except:
        author = None
        
    try:
        date = soup.find('time', class_='entry-date published').text
        date_of_publication = date_formatting(date)
    except:
        date_of_publication = None    
        

    article = soup.find('article')
    
    try:
        text_of_article = " ".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None

        
   
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_="entry-tags").find_all('a')])
   
    except:
        tags = None
    
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'cielecki', x)])
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
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links, 
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False
                             }

    all_results.append(dictionary_of_article)



#%%main
article_links = get_article_links('https://marcincielecki.wordpress.com/sitemap.xml')
 
all_results = []     
with ThreadPoolExecutor(max_workers=10) as executor:
    list(tqdm(executor.map(dictionary_of_article, article_links),
              total=len(all_results)))
       
   
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=True)


df.to_json(f'data/marcincielecki_{datetime.today().date()}.json', orient='records', force_ascii=False, indent=2)

with pd.ExcelWriter(f"data/marcincielecki_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   