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
from functions import date_change_format_long

# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive


#%% def    
def get_archive_links(link): 
    #Zbiera linki do archiwum (miesięcy)
    # link = 'https://zdaniemszota.pl/'
    
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    
    archive = soup.find('li', class_='menu-item-has-children')
    years = archive.find_all('li', class_='menu-item-has-children') 
   
    pattern = re.compile(r"/archiwum/\d{4}-\d{2}")
    
    months_links = [
       li for year in years for li in year.find_all('li')  # Iterujemy przez każdy rok i jego elementy <li>
       if li.find('a') and li.find('a').get('href') and pattern.search(li.find('a').get('href'))
    ]
    # Pobieramy tylko same URL-e
    months_urls =  [urljoin(link, li.find('a')['href']) for li in months_links]
    
    return months_urls


def get_articles_links(archive_link):
    #Uwzględnić stronicowanie postów dla poszczególnych miesięcy! 
    
    
    archive_link = 'https://zdaniemszota.pl/archiwum/2025-01'
    archive_link = 'https://zdaniemszota.pl/archiwum/2024-04'
    
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    
    links = [x for x in soup.find_all()]
    
    
    
    
    
    
    
    
    
    
def dictionary_of_article(article_link):
    # article_link = 'https://czytamaja.pl/smiech-przez-lzy-emilia-dluzewska/'
    # article_link = 'https://czytamaja.pl/niezwykle-zycie-nellie-bly-czyli-z-piorem-i-sukienka/'
    # article_link = 'https://czytamaja.pl/wyprawa-shackletona-czyli-drzyj-zaogo/'
    # article_link = 'https://czytamaja.pl/z-niejednej-poki-czyli-dugo-wyczekiwany/'
    # article_link = 'https://czytamaja.pl/ni-pies-ni-wydra-czyli-recepty-narysowane/'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
 
    author = 'Maja Sołtysik'
    
    try:
        title_of_article = soup.find('h1', class_='single-article-name').text.strip()
    except:
        title_of_article = None
   
    
        
    date = soup.find('div', class_='single-article-date').text
    date_of_publication = date_change_format_long(date)
    
    article = soup.find('div', class_='single-article-content')
    
    try:
        footer = " | ".join([x.text for x in soup.find('div', class_='single-article-ending__copy').find_all('p')])
    except:
        footer = None
    
    
    try:
        text_of_article = "".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None
    
    
    try:
        title_of_book = re.search(r'(?<=Tytuł\s\–\s).*', footer).group(0)
    except:
        title_of_book = None
    
    
    
    
    try:
        author_of_book = re.search(r'(?:Tekst(?: i ilustracje)?|Autor) – ([A-ZŁŚŻŹĆĄĘÓŃ][a-złśżźćąęóń]+(?: [A-ZŁŚŻŹĆĄĘÓŃ][a-złśżźćąęóń]+)*)', footer).group(1)
    except:
        author_of_book = None
        
        
    try:
        publishing = re.search(r'Wydawnictwo\s+[^|,\n]+(?:,\s*[^\|,\n]+)?(?:,\s*\d{4})?', footer).group(0)
    except:
        publishing = None

    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='single-article-categories').find_all('a')])
    except:
        tags = None

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|lapsusofil', x)])
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
                             'Autor dzieła': author_of_book,
                             'Tytuł dzieła': title_of_book,
                             'Adres wydawniczy': publishing,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Stopka': footer, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
months_urls = get_archive_links('https://zdaniemszota.pl/')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'data/czytamaja_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data/czytamaja_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"data/dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
# 	gfile.SetContentFile(upload_file)
# 	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   