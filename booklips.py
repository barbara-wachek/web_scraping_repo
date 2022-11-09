#%%import
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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from functions import date_change_format_short

#%% def
def get_links_of_sitemap_links_posts(link):
    link = 'https://booklips.pl/sitemap.xml'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap-pt-)(page)(-\d{4}-\d{2}\.xml)', x.text) and not re.findall(r'https\:\/\/booklips\.pl\/sitemap-misc\.xml', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_posts.extend(articles_links)
    
    return all_articles_links_posts
 
    
 
    #(sitemap-misc\.xml$)|

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
    # link = 'https://booklips.pl/adaptacje/film/zwiastun-filmu-oficer-i-szpieg-romana-polanskiego-nakreconego-na-podstawie-powiesci-roberta-harrisa/'
    # link = 'https://booklips.pl/artykuly/kaznodzieja-festiwal-przemocy-i-brutalnosci/'
    # link = 'https://booklips.pl/artykuly/wspolczesny-bajarz-terry-pratchett/'
    # link = 'https://booklips.pl/biurka-polskich-pisarzy/filip-zawada/'
    # link = 'https://booklips.pl/ciekawostki/dlaczego-henryk-sienkiewicz-otrzymal-nobla-i-jak-do-tego-doszlo-ze-nie-podzielil-sie-nagroda-z-eliza-orzeszkowa/'
    # link = 'https://booklips.pl/ciekawostki/dlaczego-a-j-finn-publikuje-pod-pseudonimem-wyjasniamy-zagadke-autora-kobiety-w-oknie/'
    # link = 'https://booklips.pl/ciekawostki/fantastyczny-wywiad-z-michelem-houellebekiem/'
    # link = 'https://booklips.pl/ciekawostki/ostatni-wiersz-charlesa-bukowskiego-przeslany-faksem/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/gra-w-pilke-ludzka-czaszka-przeczytaj-fragment-powiesci-bog-tak-chcial-arka-gieszczyka/'
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/akcja-wisla-motywem-przewodnim-nowej-powiesci-roberta-nowakowskiego-przeczytaj-przed-premiera-fragment-ojczyzny-jablek/'
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/o-dwoch-kobietach-fragment-uhonorowanej-nagroda-literacka-unii-europejskiej-powiesci-wyspa-krach-iny-wylczanowej/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/kultowa-zazi-w-metrze-raymonda-queneau-dostepna-w-ksiegarniach-przeczytaj-poczatek-ksiazki/'
    # link = 'https://booklips.pl/galeria/eros-i-tanatos-na-ilustracjach-z-1934-roku-do-kwiatow-zla-charlesa-baudelairea/'
    # link = 'https://booklips.pl/newsy/zlodziej-manuskryptow-zatrzymany-przez-fbi-od-ponad-pieciu-lat-podszywal-sie-pod-przedstawicieli-branzy-literackiej-by-zyskac-dostep-do-ksiazek-przed-premiera/'
    # link = 'https://booklips.pl/newsy/w-nowym-albumie-lucky-luke-bedzie-walczyl-z-rasizmem-na-glebokim-poludniu/'
    
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
        text_of_article = " ".join(text_of_article).strip()
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
    
        
    author = None   
    author_of_book = None
    title_of_book = None
    rating = None
    title_of_adaptation = None

    
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
            author = content_of_article.find_all('p', class_=None)[-2]
      
        if author:
            author = author.text
        else:
            author = None
         
        if re.search(r'(Ocena\:\s\d\,?\d?\s?\/\s\d{2})', text_of_article):
            rating = re.search(r'(Ocena\:\s\d\,?\d?\s?\/\s\d{2})', text_of_article).group(0).replace('Ocena: ', '')
        else:
            rating = None
            
    if category == 'czytelnia':
        if re.search(r'fragmenty-ksiazek', link):
            title_and_author_of_book = [x.text for x in content_of_article.find_all('strong') if re.findall(r'„[\p{L}\s\']*”', x.text)]
            if len(title_and_author_of_book) >= 2:
                title_and_author_of_book = "".join(title_and_author_of_book[-1].strip())
                title_of_book = re.sub(r'^(.*)(„[\p{L}\s\']*”)$', r'\2', title_and_author_of_book).strip()
                author_of_book = re.sub(r'^(.*)(„[\p{L}\s\']*”)$', r'\1', title_and_author_of_book).strip()
            else:
                title_of_book = None
                author_of_book = None
        else:
            title_of_book = None
            author_of_book = None
    

   
    if category == 'adaptacje':
        if re.search(r'\„.*”', title_of_article):
            title_of_adaptation = re.search(r'\„.*”', title_of_article).group(0)
        else: 
            title_of_adaptation = None
            
        if re.search(r'\[am\]', text_of_article):
            author = ' Artur Maszota [am]'
        elif re.search(r'\[edm\]', text_of_article):
            author = 'Emilia Dulczewska-Maszota [edm]'
        elif re.search(r'Artur Maszota', text_of_article):
            author = 'Artur Maszota [am]'
        elif re.search(r'\[kch\,am\]', text_of_article):
            author = 'Karolina Chymkowska | Artur Maszota | [kch,am]'
        elif re.search(r'\[am,mw\]', text_of_article):
            author = 'Artur Maszota | Mariusz Wojteczek | [am,mw]'
        elif re.search(r'\[aw,am\]', text_of_article):
            author = 'Anna Wyrwik | Artur Maszota | [aw,am]'
        elif re.search(r'\[mw\]', text_of_article):
            author = 'Mariusz Wojteczek [mw]'     
        else:
            author = None
    
    
    if category == 'artykuly':
        if re.search(r'(\p{Lu}\p{L}*\s\p{Lu}\p{L}*$)', text_of_article):
            author = re.search(r'(\p{Lu}\p{L}*\s\p{Lu}\p{L}*$)', text_of_article).group(0)
        else:
            author = None
            
    if category == 'biurka-polskich-pisarzy' or 'biurka-polskich-pisarzy':
        if re.search(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', title_of_article):
            related = re.sub(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', r'\4', title_of_article)
        else:
            related = None
        
            
        if re.search(r'\[am\]', text_of_article):
            author = 'Artur Maszota [am]'
        elif re.search(r'Artur Maszota', text_of_article):
            author = 'Artur Maszota [am]'
        else:
            author = None
    
    if category == ' galeria' or category == 'newsy': 
        
        if re.search(r'\[am\]', text_of_article):
            author = 'Artur Maszota [am]'
        elif re.search(r'Artur Maszota', text_of_article):
            author = 'Artur Maszota [am]'
        elif re.search(r'\[kch\,am\]', text_of_article):
            author = 'Karolina Chymkowska | Artur Maszota | [kch,am]'
        elif re.search(r'\[am,mw\]', text_of_article):
            author = 'Artur Maszota | Mariusz Wojteczek | [am,mw]'
        elif re.search(r'\[aw,am\]', text_of_article):
            author = 'Anna Wyrwik | Artur Maszota | [aw,am]'
        elif re.search(r'\[mw\]', text_of_article):
            author = 'Mariusz Wojteczek [mw]'
        elif re.search(r'\[edm\]', text_of_article):
            author = 'Emilia Dulczewska-Maszota [edm]'
        elif re.search(r'\[kch\]', text_of_article):
            author = 'Karolina Chymkowska [kch]'
        else:
            author = None
           
    
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
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap-pt-)(post)(-\d{4}-\d{2}\.xml)', x.text) and not re.findall(r'https\:\/\/booklips\.pl\/sitemap-misc\.xml', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_pages.extend(articles_links)
    
    return all_articles_links_pages    
    
#%% main
sitemap_links = ['https://booklips.pl/sitemap.xml']   

all_articles_links_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_posts, sitemap_links), total=len(sitemap_links)))       

all_results_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links_posts), total=len(all_articles_links_posts)))       


all_articles_links_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_pages, sitemap_links), total=len(sitemap_links)))  



with open(f'booklips_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        

    
df_posts = pd.DataFrame(all_results_posts).drop_duplicates()
df_posts["Data publikacji"] = pd.to_datetime(df_posts["Data publikacji"]).dt.date
df_posts = df_posts.sort_values('Data publikacji', ascending=False)
   
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

