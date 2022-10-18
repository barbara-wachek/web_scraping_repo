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
    #Wykluczenie linków, które nie prowadzą bezporednio do artykułów:
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
    #link = 'https://www.biuroliterackie.pl/biblioteka/zdjecia/zycie-na-korei/'
    #link = 'https://www.biuroliterackie.pl/biblioteka/dzwieki/bedzie/' #z audio
    #link = 'https://www.biuroliterackie.pl/biblioteka/dzwieki/egzotyczne-ptaki-i-rosliny/' #z audio
    #link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/ewangelia-brudnych-ludzi/'
    #link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/piec-esejow-homerowskich/'
    #link = 'https://www.biuroliterackie.pl/biblioteka/cykle/podsumowanie-transportu-literackiego-27/'
    #link = 'https://www.biuroliterackie.pl/biblioteka/nagrania/rozmowy-na-koniec-odcinek-3-krzysztof-chronowski/'
    #link = 'https://www.biuroliterackie.pl/biblioteka/debaty/swiat-nie-jest-do-zycia/'
    
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    section = re.sub(r'(https:\/\/www\.biuroliterackie\.pl\/)(biblioteka|biuletyn|ksiazki|projekty)(\/)(.*)', r'\2', link)
    
    #Informacja o kategorii musi być wyciągana z kilku miejsc (różna struktura wpisów w zależnosci od sekcji - po zeskrobaniu mozna bedzie scalic te kolumny Kategoria i Druga kategoria w jedną kolumnę)
    try:
        category = soup.find('span', class_='category').text
    except AttributeError:
        category = None
        
    try: 
        second_category = soup.find('span', class_='data_cat').text.strip()
    except AttributeError:
        second_category = None
    
    #Z datą podobny problem jak z kategorią. Pojawia się w różnych miejscach albo nie ma jej wcale. W artykułach pochodzących z sekcji biblioteka data pojawia sie poziom wyżej (nie na stronie konkretnego artykułu, ale na stronie z listą artykułów - dlatego data wyciągana jest osobno, w innej funkcji i potem dodawana do all_results)
    try:
        date_of_publication = re.match(r'\d{2}\/\d{2}\/\d{2}', category.strip())
        result = time.strptime(category.strip(), "%d/%m/%y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = changed_date.date().strftime("%Y-%m-%d")
    except(AttributeError, KeyError, IndexError, ValueError):
        new_date = None
        
    if section == 'biuletyn':
        date_of_publication = soup.find('span', class_='data_data').text
        result = time.strptime(date_of_publication.strip(), "%d/%m/%Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = changed_date.date().strftime("%Y-%m-%d")
        
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
    
    #Tekst wpisu:     
    text_of_article = " ".join([x.text.replace('\xa0','').replace('\n',' ').strip() for x in content_of_article.find_all('section', class_='single_right') if not re.findall(r'O AUTORZE', x.text)])
    if text_of_article == '' or None:
        try:
            text_of_article = " ".join([x.text for x in content_of_article.find('div', class_='biuletyn___post-content').findChildren('p', recursive=False) if not '\xa0' in x.text])
        except AttributeError:
            text_of_article = None
    else:
        text_of_article == None 
           
    try:
        titles_of_poems = " | ".join([x.text for x in content_of_article.find_all('h2') if not re.findall(r'(O AUTORZE)|(powiązania)|(O AUTORACH)|(Filmy autora)|(INNE GALERIE)|(inne głosy w debacie)|(Слава Україні!)|(CZYTAJ GŁOSY W DEBACIE)', x.text)])
    except(AttributeError, KeyError, IndexError):
        titles_of_poems = None 
        
    #Nota o autorze:    
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
   
   
#Funkcje do pozyskania dat publikacji wpisów z sekcji biblioteka, aby uzupełnić nimi listę all_results:  

#Funkcja do stworzenia listy wygenerowanych linków stron, na których znajduje się lista artykułów z poszczególnych kategorii sekcji biblioteka (przykład: 'https://www.biuroliterackie.pl/biblioteka/wywiady/page/1'). Nie wszystkie linki z tej listy będą zawierać informacje - niektóre będą puste - czy są puste sprawdzi funkcja poniżej - checking_content_of_links    
def web_scraping_biblioteka_by_category(link):
    format_link = re.sub(r'(https\:\/\/www\.biuroliterackie\.pl\/biblioteka\/)(.*)(\/page\/)(\d*)', r'\1\2\3', link)
    for number in range(1,300):
        link = format_link+str(number)
        all_created_links_of_biblioteka.append(link)   
   
def checking_content_of_links(link): 
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')

    try:
        soup.find('h1', class_='page-title').text =='Oops! That page can’t be found.'
    except AttributeError:
        all_available_links_of_biblioteka.append(link)
           

def links_and_dates_of_publications(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')

    list_of_links = [x.a['href'] for x in soup.find_all('span', class_='wiecej')]
    list_of_dates_of_publication = [datetime.fromtimestamp(mktime(time.strptime(e.strip(), "%d/%m/%Y"))).date().strftime("%Y-%m-%d") for e in [x.text for x in soup.find_all('span', class_='archive_date')]]
    
    list_of_links_with_dates = list(zip(list_of_links, list_of_dates_of_publication))
    all_list_of_links_with_dates_from_biblioteka.extend(list_of_links_with_dates)

def add_dates_to_all_results():     
    #record = all_results[1]
    for record in tqdm(all_results):
       for key,value in record.items(): 
           for tup in all_list_of_links_with_dates_from_biblioteka:
                if record['Link'] in tup[0]:
                    record['Data publikacji'] = tup[1]
                    
                    
    
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


#Stworzenie listy wszystkich linków ze strony (z wybranych linków mapy strony - selekcja linków wymienionych u dołu): 
all_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links, sitemaps_links), total=len(sitemaps_links)))
 
#Usunięcie duplikatów
all_links = set(all_links)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_links), total=len(all_links)))


#Uzupełnienie listy słowników (all_results) o brakujące daty publikacji w artykułach z sekcją Biblioteka

all_formats_links_of_biblioteka = ['https://www.biuroliterackie.pl/biblioteka/wywiady/page/1','https://www.biuroliterackie.pl/biblioteka/recenzje/page/1', 'https://www.biuroliterackie.pl/biblioteka/ksiazki/page/1', 'https://www.biuroliterackie.pl/biblioteka/utwory/page/1', 'https://www.biuroliterackie.pl/biblioteka/debaty/page/1', 'https://www.biuroliterackie.pl/biblioteka/cykle/page/1', 'https://www.biuroliterackie.pl/biblioteka/dzwieki/page/1', 'https://www.biuroliterackie.pl/biblioteka/nagrania/page/1', 'https://www.biuroliterackie.pl/biblioteka/zdjecia/page/1', 'https://www.biuroliterackie.pl/biblioteka/kartoteka_25/page/1']

all_created_links_of_biblioteka = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(web_scraping_biblioteka_by_category, all_formats_links_of_biblioteka), total=len(all_formats_links_of_biblioteka)))
    
all_available_links_of_biblioteka = []        
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(checking_content_of_links, all_created_links_of_biblioteka), total=len(all_created_links_of_biblioteka)))       
    
all_list_of_links_with_dates_from_biblioteka = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(links_and_dates_of_publications, all_available_links_of_biblioteka), total=len(all_available_links_of_biblioteka)))       

    
#Uruchomienie funkcji uzupełniającej luki w kolumnie Data publikacji z all_results: 
add_dates_to_all_results()

#Utworzenie obiektu DataFrame z aktualizowanej o daty listy all_results i sortowanie wg dat:
df = pd.DataFrame(all_results).drop_duplicates() 
#df['Data publikacji'].isna().value_counts() #do sprawdzenia
df = df.sort_values('Data publikacji', ascending=False)

#df_test = df[df['Tytuły wierszy'] != '']
# df['Sekcja'].value_counts()
# df['Data publikacji'].isna().value_counts()
# df[df['Sekcja'].str.contains('biuletyn') & df['Data publikacji'].isna()]
# df_notna_date = df[df["Sekcja"].str.contains("biblioteka") & df['Data publikacji'].notna()]
# df[df['Autor'].str.contains('Grzegorz Wróblewski')]    

#sprawdzenie przez podstawienie linku do artykułu (po wejsciu na strone artykulu nie widac daty)
# value = 'https://www.biuroliterackie.pl/biblioteka/debaty/poeta-dzisiaj-nie-ma-partnera-do-rozmowy/'
# for tup in all_list_of_links_with_dates_from_biblioteka:
#     if value in tup[0]:
#         print(tup[1])


#%% json i xlsx

with open(f'biuroliterackie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

with pd.ExcelWriter(f"biuroliterackie_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()  

    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"biuroliterackie_{datetime.today().date()}.xlsx", f'biuroliterackie_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



#%% Notatki
#Osobno zeskrobać po przejrzeniu?: 'https://www.biuroliterackie.pl/wydarzenia-sitemap.xml' (tu trzeba trochę pomyslec nad tym co zeskrobac i jak uporzadkowac), '

#Selekcja częsci o ksiazkach z sekcji Ksiazki (dot. oferty sklepowej)






upload_file_list = [f"biuroliterackie_{datetime.today().date()}.xlsx", f'biuroliterackie_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    





















