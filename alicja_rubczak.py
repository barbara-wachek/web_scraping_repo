#%%import
from __future__ import unicode_literals
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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%% def

def get_links_of_years(digit_from_range):
    format_url = 'https://alicjarubczak.wordpress.com/'
    working_url = f'{format_url}{digit_from_range}'
    html_text = requests.get(working_url).text
    soup = BeautifulSoup(html_text, 'lxml')
    try:
        if soup.find('div', class_='page-content').p.text == 'Wygląda na to, że niczego tu nie ma. Może spróbuj wyszukiwania?':
            pass
    except (AttributeError):
        links_of_year_pages.append(working_url)
    return links_of_year_pages


def get_all_year_content(link):
    format = '/page/'
    range_to_check = range(1,30)
    for x in range_to_check:
        url = f'{link}{format}{x}'
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'lxml')
        try:
            if soup.find('div', class_='page-content').p.text == 'Wygląda na to, że niczego tu nie ma. Może spróbuj wyszukiwania?':
                pass
        except (AttributeError):
            links_of_all_year_content.append(url)
    return links_of_all_year_content



def get_links_of_articles(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('h1', class_='entry-title')]
    links_of_all_articles.extend(links)
    return links_of_all_articles


def dictionary_of_article(link):
    #link = 'https://alicjarubczak.wordpress.com/2011/03/11/male-swieto-wiosny-2/'
    #link = 'https://alicjarubczak.wordpress.com/2012/12/30/2012-podsumowanie-i-nadzieje-na-lepszy-2013/'
    #link = 'https://alicjarubczak.wordpress.com/2012/04/21/po-mwst-slow-kilka-8-2/'
    #link = 'https://alicjarubczak.wordpress.com/2009/09/23/spotkanie-z-kasia-prawdziwa-diwa-teatrow-lalkowych/'
    html_text = requests.get(link).text
# html_collect = []
# for link in tqdm(all_links):
#     #all_links[100] = link
#     html_text = str(requests.get(link).status_code)
#     html_collect.append(html_text)
  
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    try:
        date_of_publication =  soup.find('time', class_='entry-date published').text
        result = time.strptime(date_of_publication, "%d/%m/%Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = changed_date.date().strftime("%Y-%m-%d")
    except AttributeError:
        new_date = re.sub(r'(https\:\/\/alicjarubczak\.wordpress\.com\/)(\d{4})(\/)(\d{2})(\/)(\d{2})(\/.*)', r'\2-\4-\6', link)
    
    try:    
        date_of_updating = soup.find('time', class_='updated').text
        result = time.strptime(date_of_updating, "%d/%m/%Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date_updating = changed_date.date().strftime("%Y-%m-%d")
    except(AttributeError, KeyError, IndexError):
        new_date_updating = None
        
    content_of_article = soup.find('main', class_='site-main')
    entry_content = soup.find('div',class_='entry-content')
    author = content_of_article.find('a', class_='url fn n').text
    text_of_article = " ".join([x.text.replace('\xa0','').replace('\n','') for x in entry_content.find_all('p')])
    
    title_of_article = soup.find('header', class_='entry-header').text.strip()

    
    try:
        external_links = ' | '.join([x for x in [x.get('href') for x in entry_content.find_all('a') if x.get('href') != None] if not re.findall(r'alicjarubczak|images|mail', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None

    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
       

    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date, 
                             'Data edycji': new_date_updating,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }


    all_results.append(dictionary_of_article)









#%% main

links_of_year_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_years, range(2009, (datetime.now().year)+1)), total=len(range(2009, (datetime.now().year)+1))))
                                                                                                                                               

links_of_all_year_content = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_all_year_content, links_of_year_pages), total=len(links_of_year_pages)))
  

links_of_all_articles = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_articles, links_of_all_year_content), total=len(links_of_all_year_content)))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, links_of_all_articles), total=len(links_of_all_articles)))
    
    
with open(f'alicjarubczak_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"alicjarubczak_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()  


    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"alicjarubczak_{datetime.today().date()}.xlsx", f'alicjarubczak_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    

                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               
                                                                                                                                               