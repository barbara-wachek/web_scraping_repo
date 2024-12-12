#%% import 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import get_links, date_change_format_short


#%% def    
#Wygenerowanie linków do poszczególnych podstron sekcji Kultura
def get_culture_pages(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    last_page = [e.text for e in soup.find('ul', class_='paginatorUl').find_all('li', class_='paginatorLi')][-1].strip()
    format_link = 'https://noizz.pl/kultura?page='
    
    culture_pages = []
    for number in range(int(last_page)+1):
        link = format_link + str(number)
        culture_pages.append(link)
    
    return culture_pages


def get_articles_links(link):
    link = 'https://noizz.pl/kultura?page=0'
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    # articles = [x for x in soup.find('div', class_='gridContainer gridContainer_').find_all('a', class_=' itemList gridItem')]

    articles = [x['href'] for x in soup.find_all('a', class_='itemLink')] #Za duzo odsiac te inne!












def dictionary_of_article(article_link):
    # article_link = 'https://mojaprzestrzenkultury.pl/archiwa/1722'
    # article_link = 'https://mojaprzestrzenkultury.pl/archiwa/1716'   #autor w tekscie na koncu
    # article_link = 'https://mojaprzestrzenkultury.pl/archiwa/1699'
    # article_link = 'https://mojaprzestrzenkultury.pl/archiwa/199'
    # article_link = 'https://mojaprzestrzenkultury.pl/archiwa/2900' #data inaczej
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    comments = None
    
    try:
        if soup.find('time', class_='entry-date published'):
            date_of_publication = soup.find('time', class_='entry-date published').text   
        else:
            date_of_publication = soup.find('time', class_='entry-date published updated').text 
            comments = "Data edycji, nie publikacji!"
        date_of_publication = date_change_format_short(date_of_publication)
    except:
        date_of_publication = None
    
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except:
        title_of_article = None
        
    
    try:
        category = " | ".join([e.text for e in soup.find('div', class_='entry-categories').find_all('a')])
    except:
        category = None
    
        
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='entry-tags clearfix').find_all('a')])
    except:
        tags = None
        
    
    article = soup.find('div', class_='entry-content clearfix')
    
    try:
        text_of_article = [p.text.strip().replace("\n", " ").replace("  ", " ") for p in article.find_all('p')]
        text_of_article = " ".join([' | ' if len(p) == 0 else p for p in text_of_article])          # znakiem pipe'a odzielam od siebie wiersze
    except:
        text_of_article = None
    
    
    try:
        if re.search(r'(Czas na poezję:? )(.*)', title_of_article):
            author = re.sub(r'(Czas na poezję:? )(.*)', r'\2', title_of_article).title().strip()
        else:
            author = [p for p in article.find_all('p', {'style':'text-align: right;'})][-1].text.title().strip()
           
    except:
        author = None
    
    
    try:
        tags = " | ".join([x.text for x in soup.find('div', class_='entry-tags clearfix').find_all('a')]) 
    except:
        tags = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'mojaprzestrzenkultury|addtoany', x)])
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
                             'Uwagi': comments,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                        }
        

    all_results.append(dictionary_of_article)


#%% main
culture_pages = get_culture_pages('https://noizz.pl/kultura')


articles_links = get_links('https://mojaprzestrzenkultury.pl/post-sitemap.xml')

# articles_links_yoast = get_links('https://mojaprzestrzenkultury.pl/post-sitemap.xml')  #Są dwie sitemapy, więc porownałam ilosc linków do artykułów. Obie mają tyle samo linków. 


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji')

   
with open(f'data\\moja_przestrzen_kultury_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

with pd.ExcelWriter(f"data\\moja_przestrzen_kultury_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
   
   

#%%Uploading files on Google Drive

# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)   
      
# upload_file_list = [f"data\\dakowicz_{datetime.today().date()}.xlsx", f'data\\dakowicz_{datetime.today().date()}.json']
# for upload_file in upload_file_list:
# 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
# 	gfile.SetContentFile(upload_file)
# 	gfile.Upload()  



   
   
   
   
   
   
   
   
   
   