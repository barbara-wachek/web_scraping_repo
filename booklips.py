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

from functions import date_change_format_short

#%% def



def get_links_of_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'sitemap-misc\.xml$', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links.extend(articles_links)
    
    return all_articles_links
 

def dictionary_of_article(link):
    #link = 'https://booklips.pl/newsy/zmarl-kevin-oneill-wspoltworca-serii-komiksowej-liga-niezwyklych-dzentelmenow/'
    #link = 'http://booklips.pl/czytelnia/opowiadania/nieznana-historia-milosna-pajaka-jednego-z-bohaterow-serii-kolory-zla-przeczytaj-opowiadanie-kryminalne-cytrusy-i-migdaly-malgorzaty-oliwii-sobczak/'
    #link = 'https://booklips.pl/recenzje/w-pulapce-bez-wyjscia-recenzja-ksiazki-ruiny-scotta-smitha/' #recenzja
    #link = 'https://booklips.pl/recenzje/suplement-do-masakry-recenzja-komiksu-rzeznia-numer-piec-ryana-northa-i-alberta-monteysa/' #recenzja
    #link = 'https://booklips.pl/recenzje/powrot-krola-recenzja-ksiazki-basniowa-opowiesc-stephena-kinga/' #rec
    #link = 'https://booklips.pl/recenzje/w-trybach-rewolucji-recenzja-komiksu-wolnosc-albo-smierc-aleksandry-herzyk/' #rec.
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/dluga-podroz-pewnej-opowiesci-przeczytaj-fragment-miasta-w-chmurach-anthonyego-doerra/' #czyt.
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/brutalnie-szczera-opowiesc-o-relacji-miedzy-umierajaca-matka-a-dorosla-corka-przeczytaj-fragment-ksiazki-ostatni-raz-helgi-flatland/'
    # link = 'https://booklips.pl/wywiady/przerwac-milczenie-opowiescia-rozmowa-z-carolina-de-robertis-autorka-cantoras/' #wywiad
    
    # link = 'https://booklips.pl/adaptacje/film/amazon-studios-prezentuje-drugi-zwiastun-serialu-wladca-pierscieni-pierscienie-wladzy/' #adaptacja filmowa
    #link = 'https://booklips.pl/recenzje/wykrecone-na-druga-strone-recenzja-komiksu-zasada-trojek-tomasza-spella/' # rec.
    #link = 'https://booklips.pl/recenzje/historia-o-ogromnym-potencjale-recenzja-ksiazki-czarne-skrzydla-czasu-diane-setterfield/'
    
    
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
    
    text_of_article = [x.text.replace('\n', ' ') for x in content_of_article.find_all('p', class_=None)] #class None,aby nie brać tagów i kategorii, które znajdują się pod tekstem
    if text_of_article:
        text_of_article = " ".join(text_of_article)
    else:
        text_of_article = None

    tags = content_of_article.find('p', class_='tags')
    if tags: 
        tags = ' | '.join([x.text for x in content_of_article.find('p', class_='tags').findChildren('a')])
    else:
        tags = None
          
    #buy_box = content_of_article.find(class_='bb-widget')

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
        
    author = None    #Domyslnie brak wartosci (autor pojawia sie tylko w kategorii recenzje i wtedy jest uzupelniany)
    author_of_book = None
    title_of_book = None
    rating = None

    
    #Informacje dla recenzji:
    if category == 'recenzje':
        if re.search(r'^(.*)(\„\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\”)(\,\stłum\.)', text_of_article):
            title_and_author_of_book = re.search(r'^(.*)(\„\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\”)(\,\stłum\.)', text_of_article).group(0)
            title_of_book = re.sub(r'^(.*)(\„\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\”)(\,\stłum\.)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(\„\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\,?\s?\p{L}*\”)(\,\stłum\.)', r'\1', title_and_author_of_book).strip()
        else:
            title_of_book = 'DO UZUPEŁNIENIA'
            author_of_book = 'DO UZUPEŁNIENIA'
        
        #author = [x.findChildren('strong') for x in content_of_article.find_all('em')]
        
        if content_of_article.find('span', class_='bb-widget'):
            author = content_of_article.find_all('p', class_=None)[-1]
        else:
            author = content_of_article.find_all('p', class_=None)[-1]
      
        if author:
            author = author.text
        else:
            author = NoneType
         
        if re.search(r'(Ocena\:\s\d\,?\d?\s?\/\s\d{2})', text_of_article):
            rating = re.search(r'(Ocena\:\s\d\,?\d?\s?\/\s\d{2})', text_of_article).group(0).replace('Ocena: ', '')
        else:
            rating = None
            
    if category == 'czytelnia':
        if re.search(r'(F|f)ragment', title_of_article):
            title_and_author_of_book = "".join([x.text for x in content_of_article.find_all('strong') if re.findall(r'„[\p{L}\s\']*”$', x.text)])
            if title_and_author_of_book != '':
                title_of_book = re.sub(r'^(.*)(„[\p{L}\s\']*”)$', r'\2', title_and_author_of_book)
                author_of_book = re.sub(r'^(.*)(„[\p{L}\s\']*”)$', r'\1', title_and_author_of_book)
        else:
            title_of_book = None
            author_of_book = None
            
    if category == 'wywiady':
        if re.search(r'Rozmawiał\:\s', text_of_article):
            author = ' | '.join([x.text for x in content_of_article.find_all('em') if 'Rozmawiał' in x.text])
            
   
    if category == 'adaptacje':
        if re.search(r'\„.*”', title_of_article):
            title_of_adaptation = re.search(r'\„.*”', title_of_article).group(0)
        else: 
            title_of_adaptation = None
    else:
        title_of_adaptation = None
        
    dictionary_of_article = {'Link': link, 
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book, 
                             'Tytuł adaptacji': title_of_adaptation,
                             'Ocena książki': rating,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }

    all_results.append(dictionary_of_article)
    
    
    
    
#%% main
sitemap_links = ['https://booklips.pl/sitemap.xml']   

all_articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links, sitemap_links), total=len(sitemap_links)))       

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links), total=len(all_articles_links)))       


with open(f'booklips_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        

    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f'booklips_{datetime.today().date()}.xlsx', engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f'bezprzeginania_{datetime.today().date()}.xlsx', f'bezprzeginania_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  


#NOTATKI

#Do analizy częć ksiażki i autorzy - inna struktura artykułów + wiecej informacji - nie mają dat publikacji - przeanalizować wszystklie linki, które zawierą element page
#Autor w recenzji czasem kursywą! 
# POdzielić linki z post i page na dwa arkusze i osobno zeskrobywać 

