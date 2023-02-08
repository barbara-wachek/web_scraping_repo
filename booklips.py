#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from functions import date_change_format_short

#%% def
def get_links_of_sitemap_links_posts(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap-pt-)(page)(-\d{4}-\d{2}\.xml)', x.text) and not re.findall(r'https\:\/\/booklips\.pl\/sitemap-misc\.xml', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_posts.extend(articles_links)
    
    return all_articles_links_posts
 

def dictionary_of_article_posts(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('span', class_='meta-date')
    if date_of_publication: 
        date_of_publication = date_of_publication.text 
        new_date = date_change_format_short(date_of_publication)
    else:
        new_date = None
        
    title_of_article = soup.find('h1', class_='post-title single entry-title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
        
        
    category = re.sub(r'(https?\:\/\/booklips\.pl\/)(\w*\-?\w*\-?\w*)(\/.*)', r'\2', link)
    content_of_article = soup.find('div', class_='entry')
    
    text_of_article = [x.text.replace('\n', ' ') for x in content_of_article.find_all('p', class_=None)]
    if text_of_article:
        text_of_article = " ".join(text_of_article).strip()
    else:
        text_of_article = None

    tags = content_of_article.find('p', class_='tags')
    if tags: 
        tags = ' | '.join([x.text for x in content_of_article.find('p', class_='tags').findChildren('a')])
    else:
        tags = None
          

    external_links = [x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(booklips)|(http://twitter.com/share)', x)]
    if external_links != []:
        external_links = ' | '.join(external_links)
    else:
        external_links = None
        

    photos_links = [x['src'] for x in content_of_article.find_all('img')]
    if photos_links != []:
        photos_links = ' | '.join(photos_links)
    else:
        photos_links = None
             
    
    author = None 
    for key,value in dictionary_of_authors.items():
        if key in text_of_article: 
            author = key
        elif value in text_of_article: 
            author = value



    title_of_adaptation = None
    if category == 'adaptacje':
        if re.search(r'\„.*”', title_of_article):
            title_of_adaptation = re.search(r'\„.*”', title_of_article).group(0)
        else: 
            title_of_adaptation = 'DO UZUPEŁNIENIA'
    
    
    if re.search(r'(https:\/\/booklips\.pl\/adaptacje\/)(film|muzyka|sluchowiska|teatr|gry)(\/.*)', link):
        type_of_adaptation = re.sub(r'(https:\/\/booklips\.pl\/adaptacje\/)(film|muzyka|sluchowiska|teatr|gry)(\/.*)', r'\2', link)
    else:
        type_of_adaptation = None


    title_of_book = None
    author_of_book = None
    if category == 'recenzje':
        if re.search(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', text_of_article):
            title_and_author_of_book = re.search(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', text_of_article).group(0)
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', r'\1', title_and_author_of_book).strip()
            
        else:
            title_of_book = 'DO UZUPEŁNIENIA'
            author_of_book = 'DO UZUPEŁNIENIA'
       
    
    if category == 'premiery-i-zapowiedzi':
        if re.search(r'„.*”', title_of_article):
            title_of_book = re.search(r'„.*”', title_of_article).group(0)

    
    #if re.search(r'fragmenty-ksiazek', link):
    if category == 'czytelnia':
        title_and_author_of_book = [x.text for x in content_of_article.find_all('strong') if re.findall(r'„[\p{L}\s\'\.\?\d\–\-]*”', x.text)]
        if len(title_and_author_of_book) >= 2:
            title_and_author_of_book = "".join(title_and_author_of_book[-1].strip())
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\1', title_and_author_of_book).strip()
        elif len(title_and_author_of_book) == 1:
            title_and_author_of_book = "".join(title_and_author_of_book)
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\1', title_and_author_of_book).strip()
        else:
            title_of_book = 'DO UZUPEŁNIENIA'
            author_of_book = 'DO UZUPEŁNIENIA'
   
    if re.search(r'czytelnia\/(listy|przedruki|opowiadania|wiersze|fragmenty-ksiazek)', link):   
        contributor = author
        author = None
    else:
        contributor = None    
        
        
    rating = None        
    if re.search(r'(Ocena\:?\s\d\,?\d?\s?\/\s\d{2})', text_of_article):
        rating = re.search(r'(Ocena\:?\s\d\,?\d?\s?\/\s\d{2})', text_of_article).group(0).replace('Ocena', '').replace(':',"").strip()
    else:
        rating = None
    
    
    related = None       
    if re.search(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', title_of_article):
        related = re.sub(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', r'\4', title_of_article)
    elif re.search(r'(https:\/\/booklips\.pl\/przeglad\/)([\w\-]*)(\/.*)', link):
        related = re.sub(r'(https:\/\/booklips\.pl\/przeglad\/)([\w\-]*)(\/.*)', r'\2', link)
    else:
        related = None
           
    
    dictionary_of_article = {'Link': link, 
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Współtwórca': contributor,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Autor książki/dzieła': author_of_book,
                             'Tytuł książki/dzieła': title_of_book, 
                             'Typ adaptacji': type_of_adaptation,
                             'Tytuł adaptacji': title_of_adaptation,
                             'Wpis dotyczy': related,
                             'Ocena książki': rating,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }

    all_results_posts.append(dictionary_of_article)
    


def get_links_of_sitemap_links_pages(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap)(-pt-post|-misc)(-\d{4}-\d{2})?(\.xml)', x.text)] 

    for link in tqdm(links):
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_pages.extend(articles_links)
            
    return all_articles_links_pages    

def links_of_pages_without_unnecessaries(link):
    if not re.search(r'^https:\/\/booklips\.pl\/(autorzy|wydawcy|ksiazki|katalog|komiksy|zasady-korzystania|reklama|regulamin-konkursow|regulamin-konkursow-na-facebooku-twitterze-instagramie|o-nas|redakcja)\/\w?\d?\/?$', link):
        all_articles_links_pages_without_unnecessaries.append(link)
       
    return all_articles_links_pages_without_unnecessaries



def dictionary_of_article_pages(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
        
    title_of_article = soup.find('h1', class_='page-title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
        
        
    category = re.search(r'(?<=https?\:\/\/booklips\.pl\/)(\w*\-?\w*\-?\w*)(?=\/.*)', link)
    if category: 
        category = re.search(r'(?<=https?\:\/\/booklips\.pl\/)(\w*\-?\w*\-?\w*)(?=\/.*)', link).group(0)
    else:
        category = None
        
        
    content_of_article = soup.find('div', class_='entry')
    
    text_of_article = [x.text.replace('\n',' ') for x in content_of_article.find_all('p', class_=None)]
    if text_of_article:
        text_of_article = " ".join(text_of_article).strip()
    else:
        text_of_article = None
          

    external_links = [x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(booklips)|(http://twitter.com/share)', x)]
    if external_links != []:
        external_links = ' | '.join(external_links)
    else:
        external_links = None
        

    photos_links = [x['src'] for x in content_of_article.find_all('img')]
    if photos_links != []:
        photos_links = ' | '.join(photos_links)
    else:
        photos_links = None
             
    

    title_of_book = None
    author_of_book = None
    isbn = None
    description_of_book = None
    year_of_publication = None
    
    if text_of_article != None and (category == 'ksiazki' or category == 'komiksy'): 

        if re.search(r'(?<=.cenariusz i rysunki:\s|Autor:\s|.cenariusz:\s|Autorzy:\s)(\p{L}*\.?\-?\s\p{L}*\.?\,?\-?\s*\p{L}*\s*\p{L}*\,?\s*\p{L}*\s*)(?=Tłumaczenie:|.ysunki:|Współpraca:|Kolor:|Ilustracje:|Wstęp:)', text_of_article):
            author_of_book = re.search(r'(?<=.cenariusz i rysunki:\s|Autor:\s|.cenariusz:\s|Autorzy:\s)(\p{L}*\.?\-?\s\p{L}*\.?\,?\-?\s*\p{L}*\s*\p{L}*\,?\s*\p{L}*\s*)(?=Tłumaczenie:|.ysunki:|Współpraca:|Kolor:|Ilustracje:|Wstęp:)', text_of_article).group(0).strip()
        else:
            author_of_book = 'DO UZUPEŁNIENIA'
            
        title_of_book = title_of_article
        
        if re.search(r'(?<=Opis:)(.*)(?=Materiały o .*)', text_of_article):
            description_of_book = re.search(r'(?<=Opis:)(.*)(?=Materiały o .*)', text_of_article).group(0).strip()
            
        if re.search(r'(?<=ISBN )[\d\-]*', text_of_article):
            isbn = re.search(r'(?<=ISBN )[\d\-]*', text_of_article).group(0)
            
        if re.search(r'(?<=Rok wydania:\s)\w*\s\d{4}', text_of_article):
            year_of_publication = re.search(r'(?<=Rok wydania:\s)\w*\s\d{4}', text_of_article).group(0)
        
    dictionary_of_article = {'Link': link, 
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book, 
                             'Rok i miesiąc wydania': year_of_publication,
                             'Opis książki': description_of_book,
                             'ISBN': isbn,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links
                             }

    all_results_pages.append(dictionary_of_article)    
    
#%% main
sitemap_links = ['https://booklips.pl/sitemap.xml']   

dictionary_of_authors = {'[am]': 'Artur Maszota', 
                         '[kch]': 'Karolina Chymkowska',
                         '[mw]': 'Mariusz Wojteczek',
                         '[edm]': 'Emilia Dulczewska-Maszota',
                         '[aw]': 'Anna Wyrwik', 
                         '[kch,am]': 'Karolina Chymkowska | Artur Maszota',
                         '[am,kch]': 'Artur Maszota | Karolina Chymkowska',
                         '[am,mw]': 'Artur Maszota | Mariusz Wojteczek',
                         '[aw,am]': 'Anna Wyrwik | Artur Maszota',
                         '[pd]': 'Paweł Deptuch',
                         '[ms]': 'Mirosław Skrzydło',
                         '[mss]': 'Mirosław Szyłak-Szydłowski',
                         '[pj]': 'Paulina Janota',
                         '[sr]': 'Sebastian Rerak',
                         '[mb]': 'Milena Buszkiewicz lub Maciej Bachorski',
                         '[tm]': 'Tomasz Miecznikowski',
                         '[bs]': 'Błażej Szymankiewicz',
                         '[mw,am]': 'Mariusz Wojteczek | Artur Maszota',
                         '[md]': '[md]',
                         '[em]': '[em]',
                         '[ks]': '[ks]',
                         'Katarzyna Figiel': 'Katarzyna Figiel',
                         'Marcin Waincetel' : 'Marcin Waincetel',
                         'Bartłomiej Paszylk': 'Bartłomiej Paszylk',
                         'Krzysztof Stelmarczyk': 'Krzysztof Stelmarczyk',
                         'Milena Buszkiewicz': 'Milena Buszkiewicz',
                         'Maciej Bachorski': 'Maciej Bachorski',
                         'Natalia Hennig': 'Natalia Hennig',
                         'Dawid Wiktorski':'Dawid Wiktorski',
                         'Ewelina Dyda': 'Ewelina Dyda',
                         'Rafał Siemko': 'Rafał Siemko',
                         'Kasper Linge': 'Kasper Linge',
                         'Łukasz Kamiński': 'Łukasz Kamiński',
                         'Katarzyna Lasek': 'Katarzyna Lasek',
                         'Przemysław Gulda': 'Przemysław Gulda'
                         }

all_articles_links_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_posts, sitemap_links), total=len(sitemap_links)))       

all_results_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article_posts, all_articles_links_posts), total=len(all_articles_links_posts)))       


all_articles_links_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_pages, sitemap_links), total=len(sitemap_links)))  


all_articles_links_pages_without_unnecessaries = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(links_of_pages_without_unnecessaries, all_articles_links_pages), total=len(all_articles_links_pages)))  


all_results_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article_pages, all_articles_links_pages_without_unnecessaries), total=len(all_articles_links_pages_without_unnecessaries)))       



with open(f'booklips_posts_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results_posts, f, ensure_ascii=False)        
with open(f'booklips_pages_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results_pages, f, ensure_ascii=False)       

    
df_posts = pd.DataFrame(all_results_posts).drop_duplicates()
df_posts["Data publikacji"] = pd.to_datetime(df_posts["Data publikacji"]).dt.date
df_posts = df_posts.sort_values('Data publikacji', ascending=False)

df_pages = pd.DataFrame(all_results_pages).drop_duplicates()

   
with pd.ExcelWriter(f'booklips_{datetime.today().date()}.xlsx', engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df_posts.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    df_pages.to_excel(writer, 'Pages', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f'booklips_{datetime.today().date()}.xlsx', f'booklips_posts_{datetime.today().date()}.json', f'booklips_pages_{datetime.today().date()}.json']

for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  






