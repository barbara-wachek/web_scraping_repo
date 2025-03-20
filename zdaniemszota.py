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

# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive


#%% def    
def get_archive_links(link): 
    #Zbiera linki do archiwum (miesięcy)
    
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    
    archive = soup.find('li', class_='menu-item-has-children')
    years = archive.find_all('li', class_='menu-item-has-children') 
   
    pattern = re.compile(r"/archiwum/\d{4}-\d{2}")
    
    months_links = [
       li for year in years for li in year.find_all('li')
       if li.find('a') and li.find('a').get('href') and pattern.search(li.find('a').get('href'))
    ]
    months_urls =  [urljoin(link, li.find('a')['href']) for li in months_links]
    
    return months_urls


def get_articles_links(archive_link):
    format_link = 'https://zdaniemszota.pl'
    
    html_text_sitemap = requests.get(archive_link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    
    links = [urljoin(format_link, x.a.get('href')) for x in soup.find_all('h1', class_='entry-title')]
    articles_links.extend(links)
    
    next_element = soup.find('a', {'rel': 'next'})
    next_href = next_element['href'] if next_element else None
    
    while next_href: 
        html_text_sitemap = requests.get(urljoin(format_link, next_href)).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        
        links = [urljoin(format_link, x.a.get('href')) for x in soup.find_all('h1', class_='entry-title')]
        articles_links.extend(links)
        
        next_element = soup.find('a', {'rel': 'next'})
        next_href = next_element['href'] if next_element else None
    
    return articles_links
    
    
def dictionary_of_article(article_link):
    # article_link = 'https://zdaniemszota.pl/5268-wiersz-nocna-pora-ija-kiwa-osiem-lat-mowic-tlum-aneta-kaminska'
    # article_link = 'https://zdaniemszota.pl/4944-ksiazka-tygodnia-maggie-shipstead-wielki-krag'
    # article_link = 'https://zdaniemszota.pl/3268-panna-doktor-sadowska-zapowiedz'
    # article_link = 'https://zdaniemszota.pl/3263-fragment-ksiazki-panna-doktor-sadowska-mowi-idzcie-tlumnie-do-urn-wyborczych'
    # article_link = 'https://zdaniemszota.pl/2368-premiera-ksiazki-lukier-malwiny-pajak'
    # article_link = 'https://zdaniemszota.pl/570-marta-masada-swieto-trabek'
    # article_link = 'https://zdaniemszota.pl/448-buforowanie-janusz-rudnicki-o-bombach-i-lejach-w-orwo-antologia'
    # article_link = 'https://zdaniemszota.pl/37-iwona-chmielewska-kim-heekyoung-maum'
    # article_link = 'https://zdaniemszota.pl/519-agata-tuszynska-loncia-jamnikarium'
    # article_link = 'https://zdaniemszota.pl/310-buforowanie-wislawa-szymborska-kornel-filipowicz-najlepiej-w-zyciu-ma-twoj-kot'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')

    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
   
    
    try:   
        footer = soup.find('span', class_='post-written-by').text.strip()
        if re.search(r'\d{2}\.\d{2}\.\d{4}', footer):
            date_of_publication = re.sub(r'(?s).*?(\d{2})\.(\d{2})\.(\d{4}).*', r'\3-\2-\1', footer).strip()
        else:
            date_of_publication = None
    except:
        date_of_publication = None
    
    article = soup.find('div', class_='single-article-content')
    
    
    try:   
        author = re.search(r'^(.*?),', footer, re.MULTILINE).group(1).strip()
    except:
        author = None
    
    try:
        category = " | ".join([x.text for x in soup.find('span', class_='post-written-by').find_all('a') if re.search(r'\/kategoria\/.*', x['href'])])
    except:
        category = None
    
    try:
        genre = " | ".join([x.text for x in soup.find('span', class_='post-written-by').find_all('a') if re.search(r'\/gatunek\/.*', x['href'])])
    except:
        genre = None
    
    try:
        title_of_masterpiece = " | ".join(re.findall(r'\"[^"]*\"', title_of_article))
    except:
        title_of_masterpiece = None
      
    
    author_of_masterpiece = None   
    try:
        if title_of_masterpiece:
            author_of_masterpiece = re.search(r'^([\p{L}-]+(?:\s[\p{L}-]+)*(?:,\s*[\p{L}-]+(?:\s[\p{L}-]+)*)*)\s*,\s*".+?"|".+?"\s*,\s*([\p{L}-]+(?:\s[\p{L}-]+)*(?:,\s*[\p{L}-]+(?:\s[\p{L}-]+)*)*)$', title_of_article).group(1).replace('BUFOROWANIE - ', '').replace(',', " | ")
        else: 
            author_of_masterpiece = None
    except:
        author_of_masterpiece = None

    
    article = soup.find('div', class_='entry-content')
    
    try:
        text_of_article = "".join([x.text for x in article.find_all('p')])
    except:
        text_of_article = None


    try:
        tags = " | ".join([x.text for x in soup.find('h6').find_all('a')])
    except:
        tags = None

        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'zdaniemszota', x)])
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
                             'Gatunek': genre,
                             'Autor utworu': author_of_masterpiece,
                             'Tytuł utworu': title_of_masterpiece, 
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Filmy': True if article and article.find_all('iframe') else False,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
months_urls = get_archive_links('https://zdaniemszota.pl/')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links, months_urls),total=len(months_urls)))

without_duplicates = list(set(articles_links))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, without_duplicates),total=len(without_duplicates)))

with open(f'data/zdaniemszota_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')
   
with pd.ExcelWriter(f"data/zdaniemszota_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"data/dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
# 	gfile.SetContentFile(upload_file)
# 	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   