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

def get_links_of_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    #Wykluczenie przy okazji linków, które nie prowadzą bezporednio do artykułów:
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(\/biuletyn\/$)|(\/recenzje\/$)|(\/ksiazki\/$)|(\/utwory\/$)|(\/nagrania\/$)|(\/debaty\/$)|(\/wywiady\/$)|(\/kartoteka\_25\/$)|(\/cykle\/$)|(\/dzwieki\/$)|(\/zdjecia\/$)|(\/projekty\/$)', x.text)]
    all_links.extend(links)
    
    
def dictionary_of_article(link):
    #link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/spalanie-grzegorza-kwiatkowskiego-2/' #recenzja #WAŻNE: brak daty
    #link = 'https://www.biuroliterackie.pl/biblioteka/cykle/7-siedmiu-moich-portowych-autorow-martyna-bulizanska-trauma/' #felieton z cyklu
    #link = 'https://www.biuroliterackie.pl/biuletyn/nowe-glosy-europy-zoran-pilic/' #biuletyn
    #link = 'https://www.biuroliterackie.pl/biuletyn/bronka-nowicka-podrozy/' #biuletyn
    #link = 'https://www.biuroliterackie.pl/biblioteka/ksiazki/tlen/' #inna struktura danych
    #link = 'https://www.biuroliterackie.pl/biblioteka/ksiazki/hurtownia-ran-i-wiersze-ludowe/' #ksiazki (z wierszami)
    #link = 'https://www.biuroliterackie.pl/biblioteka/utwory/z-raptularza/'
    #link = 'https://www.biuroliterackie.pl/projekty/publikuj-w-bibliotece/' #projekty
    #link = 'https://www.biuroliterackie.pl/biuletyn/biblioteka-nr-16/' #biblioteka nr x
    #link = 'https://www.biuroliterackie.pl/ksiazki/bach-for-my-baby-7/' #ksiązki poddane selekcji
    # link = 'https://www.biuroliterackie.pl/biblioteka/zdjecia/zycie-na-korei/'
    # link = 'https://www.biuroliterackie.pl/biblioteka/dzwieki/bedzie/' #z audio
    # link = 'https://www.biuroliterackie.pl/biblioteka/dzwieki/egzotyczne-ptaki-i-rosliny/' #z audio
    # link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/ewangelia-brudnych-ludzi/'
    # link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/piec-esejow-homerowskich/'
    # link = 'https://www.biuroliterackie.pl/biblioteka/cykle/podsumowanie-transportu-literackiego-27/'
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    

    section = re.sub(r'(https:\/\/www\.biuroliterackie\.pl\/)(biblioteka|biuletyn|ksiazki|projekty)(\/)(.*)', r'\2', link)
    
    try:
        category = soup.find('span', class_='category').text
    except AttributeError:
        category = None
        
    try: 
        second_category = soup.find('span', class_='data_cat').text.strip()
    except AttributeError:
        second_category = None
    
    try:
        date_of_publication = re.match(r'\d{2}\/\d{2}\/\d{2}', category.strip())
        if date_of_publication: 
            result = time.strptime(category.strip(), "%d/%m/%y")
            changed_date = datetime.fromtimestamp(mktime(result))   
            new_date = changed_date.date().strftime("%Y-%m-%d")
        else: 
            date_of_publication = soup.find('span', class_='data_data').text
            result = time.strptime(date_of_publication.strip(), "%d/%m/%Y")
            changed_date = datetime.fromtimestamp(mktime(result))   
            new_date = changed_date.date().strftime("%Y-%m-%d")
            
    except(AttributeError, KeyError, IndexError):
        new_date = None       
        
    try:    
        excerpt = soup.find('p', class_='excerpt').text.strip()
    except(AttributeError, KeyError, IndexError):
        excerpt = None  
    
    try:    
        author = soup.find('h4', class_='title_autor').text
    except AttributeError:
        author = None
        
    try:    
        title_of_article = soup.find('h1', class_='title_h1').text.strip()
    except AttributeError:
        try:
            title_of_article = soup.find('h4', class_='biuletyn_title').text.strip()
        except AttributeError:
            title_of_article = None
        
    content_of_article = soup.find('main', class_='site-main')
    

#TEKST ARTYKUŁU: 
    text_of_article = " ".join([x.text.replace('\xa0','').replace('\n',' ').strip() for x in content_of_article.find_all('section', class_='single_right') if not re.findall(r'O AUTORZE', x.text)])
    if text_of_article == '' or None:
        try:
        #text_of_article = " ".join([x.text.replace('\xa0','').replace('\n','').strip() for x in content_of_article.find_all('p')])
            text_of_article = " ".join([x.text for x in content_of_article.find('div', class_='biuletyn___post-content').findChildren('p', recursive=False) if not '\xa0' in x.text])
        except AttributeError:
            text_of_article = None
    else:
        text_of_article == None 
#TYTUŁY WIERSZY (KATEGORIA KSIĄŻKI):    
    try:
        titles_of_poems = " | ".join([x.text for x in content_of_article.find_all('h2') if not re.findall(r'(O AUTORZE)|(powiązania)', x.text)])
    except(AttributeError, KeyError, IndexError):
        titles_of_poems = None 
        
        
    try:
        about_author = content_of_article.find('p', class_='o_autorze_bio').text
    except(AttributeError, KeyError, IndexError):
        about_author = None 
        
    try: 
        author_photo = content_of_article.find('div', class_='o_autorze_left').img['src']
    except(AttributeError, KeyError, IndexError, TypeError):
        author_photo = None 
    
    try:
        external_links = ' | '.join([x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'biuroliterackie|images|mail|#', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None

    try:
        series_link = soup.find('span', class_='kategoria_debaty').a['href']
        series_name = soup.find('span', class_='kategoria_debaty').text.strip()
    except (AttributeError, KeyError, IndexError):
        series_link = None
        series_name = None
    
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        
        
    try: 
        program_link = [e for e in [x['href'] for x in content_of_article.find_all('a')] if '.mp3' in e][0]
    except (AttributeError, KeyError, IndexError):
        program_link = None 
        
    try: 
        title_of_journal = " ".join(["biBLioteka. Magazyn Literacki" for x in soup.find_all('a') if "https://www.biuroliterackie.pl/biblioteka" == x['href']])
    except (AttributeError, KeyError, IndexError):
        title_of_journal = None 
        
    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Tytuł czasopisma': title_of_journal,
                             'Sekcja': section,
                             'Kategoria': category,
                             'Dodatkowa kategoria': second_category,
                             'Autor': author,
                             'Nota o autorze': about_author,
                             'Zdjęcie autora': author_photo,
                             'Tytuł artykułu': title_of_article,
                             'Wyimek': excerpt,
                             'Tekst artykułu': text_of_article,
                             'Tytuły wierszy': titles_of_poems,
                             'Nazwa cyklu': series_name,
                             'Link do cyklu': series_link,
                             'Link do pliku audio': program_link,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }


    all_results.append(dictionary_of_article)
    
    
    
#%% main

#sitemap_link = 'https://www.biuroliterackie.pl/sitemap_index.xml'
#Wybrane linki z sitemap_link:
sitemaps_links = ['https://www.biuroliterackie.pl/ksiazki-sitemap.xml',
                  'https://www.biuroliterackie.pl/utwory-sitemap.xml',
                  'https://www.biuroliterackie.pl/nagrania-sitemap.xml',
                  'https://www.biuroliterackie.pl/debaty-sitemap1.xml',
                  'https://www.biuroliterackie.pl/debaty-sitemap2.xml',
                  'https://www.biuroliterackie.pl/wywiady-sitemap.xml',
                  'https://www.biuroliterackie.pl/recenzje-sitemap1.xml',
                  'https://www.biuroliterackie.pl/recenzje-sitemap2.xml',
                  'https://www.biuroliterackie.pl/kartoteka_25-sitemap.xml',
                  'https://www.biuroliterackie.pl/felietony-sitemap.xml',
                  'https://www.biuroliterackie.pl/dzwieki-sitemap.xml',
                  'https://www.biuroliterackie.pl/zdjecia-sitemap.xml',
                  'https://www.biuroliterackie.pl/biuletyn-sitemap.xml',
                  'https://www.biuroliterackie.pl/projekty-sitemap.xml']


#Stworzenie listy wszystkich linków ze strony (z wybranych linków mapy strony - bez linków wyimienionych u dołu): 
all_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links, sitemaps_links), total=len(sitemaps_links)))

#Sprawdzenie, czy nie ma duplikatów:
    #len(all_links) # 6436
    #len(set(all_links)) #6427
    
#Usunięcie duplikatów
all_links = set(all_links)
print(f'Liczba artykułóW: {len(all_links)}')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_links), total=len(all_links)))

with open(f'biuroliterackie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results).drop_duplicates()

with pd.ExcelWriter(f"biuroliterackie_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()  



#Osobno zeskrobać po przejrzeniu?: 'https://www.biuroliterackie.pl/wydarzenia-sitemap.xml' (tu trzeba trochę pomyslec nad tym co zeskrobac i jak uporzadkowac), 'https://www.biuroliterackie.pl/autorzy_lista-sitemap.xml' (zeskrobać zdjęcie + bibliogafię do osobnego arkusza w Excelu)

#Selekcja częsci o ksiazkach z sekcji Ksiazki (dot. oferty sklepowej)

#Problem: czesc artykułow z sekcji Biblioteka nie ma daty publikacji wewnatrz artykuly (ta data jest jak przejdzie sie poziom wyzej, tak jest w przypadku recenzji) - data jest istotna z perspektywy ustalenia numeracji czasopisma


    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"biuroliterackie_{datetime.today().date()}.xlsx", f'biuroliterackie_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    





















