#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_short
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%def

def get_links_from_sitemap(link):   
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.search(r'https\:\/\/www.zamekczyta.pl\/$', e.text)]
    articles_links.extend(sitemap_links)   

def get_category_links_from_sitemap(link): 
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.search(r'https\:\/\/www.zamekczyta.pl\/$', e.text)]
    category_links.extend(sitemap_links)       

def generate_pages(link): 
    format_link = re.sub(r'(https\:\/\/www\.zamekczyta\.pl\/)([\w\-]*)(\/page\/)(\d+)(\/)', r'\1\2\3\4\5', link)
          
    for number in range(1,30):
        link = format_link+r'page/'+str(number)
        generated_category_pages.append(link)  
    

def verify_generated_pages(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    if not soup.find('p', class_='not-found'):
        verified_generated_pages.append(link)
        
        
def get_articles_links_with_category(link):
    category = re.search(r'(?<=https\:\/\/www\.zamekczyta\.pl\/)([\w\-]*)(?=\/.*)', link).group(0)
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    articles_links = [x.a['href'] for x in soup.find_all('h2')]
    articles_links_with_category = [{x:category} for x in articles_links]
    return all_articles_links_with_category.extend(articles_links_with_category)


def dictionary_of_article(link):    
    
    # link = 'https://www.zamekczyta.pl/stala-joanna-lesiewicz/'
    # link = 'https://www.zamekczyta.pl/biale-niewolnice-o-ksiazce-sluzace-do-wszystkiego/'
    #link = 'https://www.zamekczyta.pl/poznan-poetow-piotr-macierzynski-ksiazka-kostnicy/'
    
    
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    date_of_publication = soup.find('p', class_='single-image-date')
    if date_of_publication:
        new_date = date_change_format_short(date_of_publication.text)
    else:
        new_date = None
        
    
    title_of_article = soup.find('h1')
    if title_of_article:
        title_of_article = soup.find('h1').text.strip()
    else:
        title_of_article = None


    author = soup.find_all('p', attrs={'style':'text-align: right;'}) 
    if author:
        author =' | '.join([x.text.title().replace('\n', ' | ') for x in soup.find_all('p', attrs={'style':'text-align: right;'}) if x.text != '\xa0'])
    else:
        author = None
    
    
    content_of_article = soup.find('article')
    
    if content_of_article:
        text_of_article = [x.text for x in content_of_article.find_all('p', class_=None) if x.text != '\xa0']
        if text_of_article:
            text_of_article = " | ".join([x.text.replace('\xa0', '').replace('\n', '') for x in content_of_article.find_all('p', class_=None) if x.text != '\xa0'])
        else:
            text_of_article = None
    else:
        text_of_article = None
        
   
    
    if content_of_article:  
        tags = [x.text for x in content_of_article.find_all('a') if re.search(r'\/tag\/', x['href'])]
        if tags != '':
            tags = " | ".join([x.text for x in content_of_article.find_all('a') if re.search(r'\/tag\/', x['href'])])
        else:
            tags = None
    

    about_authors = [x.text.replace('\xa0', ' ') for x in content_of_article.find_all('p') if re.match(r'^\p{Lu}{2,}\s[\p{Lu}\s\-]*(\–|\()', x.text)]
    if about_authors != '':
        about_authors = " | ".join([x.text.replace('\xa0', ' ') for x in content_of_article.find_all('p') if re.match(r'^\p{Lu}{2,}\s[\p{Lu}\s\-]*(\–|\()', x.text)])
    else:
        about_authors = None
    
    
    category = None
    for element in all_articles_links_with_category:
        for k,v in element.items():
            if k == link:
                if category:     
                    category = f'{category} | {v}'
                else:
                    category = v
    

    book_description = [x.text.replace('\xa0', ' ').strip() for x in content_of_article.find_all('p') if re.match(r'^(\„|\p{Lu}\p{Ll}).*\„?.*\”?.*\d{4}.?$', x.text)]
    if book_description != '':
        book_description = " | ".join([x.text.replace('\xa0', ' ').strip() for x in content_of_article.find_all('p') if re.match(r'^(\„|\p{Lu}\p{Ll}).*\„.*\”.*\d{4}.?$', x.text)])
    else:
        book_description = None

    

    if book_description != None:
        title_of_book = re.findall(r'(\„[\p{L}\d\s\=\.\,\?\!\-]*\”)', book_description)
        if title_of_book:
            title_of_book = " | ".join(re.findall(r'(\„[\p{L}\d\s\=\.\,\?\!\-]*\”)', book_description))
        else:
            title_of_book = None
    
        edition_year = re.findall(r'2\d{3}', book_description)
        if edition_year:
            edition_year = " | ".join(re.findall(r'2\d{3}', book_description))
        else:
            edition_year = None


    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'#|zamekczyta', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    

    
    
    if re.search(r'(wiersze$)|(wiersz$)', title_of_article):    
        titles_of_poems = ' | '.join([x.text for x in content_of_article.find_all('strong')])
    else: 
        titles_of_poems = None
    

    if author==None and re.search(r'(^Seryjni)|(^PP)', title_of_article):
        author = re.search(r'(?<=Seryjni Poeci\:\s)(.*)(?=\swiersz|wiersze)', title_of_article)
        if author: 
            author = re.search(r'(?<=Seryjni Poeci\:\s)(.*)(?=\swiersz|wiersze)', title_of_article).group(0)
        elif re.search(r'(?<=PP\s\d{4}\:)(.*)(?=\swiersz|wiersze)', title_of_article):
            author = re.search(r'(?<=PP\s\d{4}\:)(.*)(?=\swiersz|wiersze)', title_of_article).group(0)
             
    if author != None and re.search(r'\p{L}*\s\p{L}*\s\–\s(trzy|dwa)', author):
        author = re.sub(r'(\p{L}*\s\p{L}*)(\s\–\s)(trzy|dwa)', r'\1', author).strip()         
             
             
    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Opis książki': book_description,
                             'Tytuł książki': title_of_book,
                             'Tytuły wierszy': titles_of_poems,
                             'Rok wydania': edition_year,
                             'Tagi': tags,
                             'Nota u autorach/twórcach': about_authors,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)    
    
    
    
    
#%% main

sitemap_post = 'https://www.zamekczyta.pl/post-sitemap.xml'
sitemap_page = 'https://www.zamekczyta.pl/page-sitemap.xml'


articles_links = []
get_links_from_sitemap('https://www.zamekczyta.pl/post-sitemap.xml')

category_links = []
get_category_links_from_sitemap('https://www.zamekczyta.pl/category-sitemap.xml')

generated_category_pages = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(generate_pages, category_links),total=len(category_links)))

verified_generated_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(verify_generated_pages, generated_category_pages),total=len(generated_category_pages)))

all_articles_links_with_category = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links_with_category, verified_generated_pages),total=len(verified_generated_pages)))
#Niektóre linki się powtarzają - artykuły są przyporzadkowane do więcej niż jednej kategorii. W funkcji dictionary_of_article do zmiennej category są brane wszystke kategorie odpowiadające linkowi. 


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)


with open(f'zamek_czyta_posts{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
     
   
with pd.ExcelWriter(f'zamek_czyta_{datetime.today().date()}.xlsx', engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f'zamek_czyta_{datetime.today().date()}.xlsx', f'zamek_czyta_posts{datetime.today().date()}.json']

for upload_file in upload_file_list:
 	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
 	gfile.SetContentFile(upload_file)
 	gfile.Upload()  













