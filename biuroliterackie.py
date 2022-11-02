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

def get_links_of_sitemap_links_biuletyn(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(\/biuletyn\/$)', x.text)]
    all_links_from_biuletyn.extend(links)
    
    
def dictionary_of_article_from_biuletyn(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    section = re.sub(r'(https:\/\/www\.biuroliterackie\.pl\/)(biblioteka|biuletyn|ksiazki|projekty)(\/)(.*)', r'\2', link)     
    category = soup.find('span', class_='data_cat').text.strip()
    
    date_of_publication = soup.find('span', class_='data_data').text
    result = time.strptime(date_of_publication.strip(), "%d/%m/%Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = changed_date.date().strftime("%Y-%m-%d")
    
    title_of_article = soup.find('h4', class_='biuletyn_title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()
   
    content_of_article = soup.find('main', class_='site-main')
    
    try:
        text_of_article = " ".join([x.text for x in content_of_article.find('div', class_='biuletyn___post-content').findChildren('p', recursive=False) if x.text and x.text != '\xa0'])
    except AttributeError:
        text_of_article = None

    external_links = [x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'biuroliterackie|images|mail|#', x)]
    if external_links: 
        external_links = ' | '.join(external_links)
    else:
        external_links = None    
        
    photos_links = [x['src'] for x in content_of_article.find_all('img')]  
    if photos_links:
        photos_links = ' | '.join(photos_links)
          
    #Related:    
    if category == 'w bibliotece' and re.match('^(b|B)iBLioteka.*', title_of_article):
        related = title_of_article
    elif category == 'projekty' and 'Połów' in title_of_article:
        related = re.sub(r'(^Połów\s\d{4})(.*)', r'\1', title_of_article)
    elif category == 'premiery':
        related = re.sub(r'(BL-e premiera \/ )?(Poezja z nagrodami\:)?(.*\:?.*)', r'\3', title_of_article).strip()
    elif 'Stacja' in category or 'Stacja' in title_of_article:
        related = category
    elif 'TransPort' in category or 'TransPort' in title_of_article:
        related = category
    elif 'Poezja z nagrodami' in title_of_article:
        related = re.sub(r'(Poezja z nagrodami\:)(.*)', r'\2', title_of_article).strip()
    elif category == 'wieści z biura':
        related = None
    elif category == 'zapowiedzi' and 'Zapowiedź z Biura' in title_of_article:
        related = re.sub(r'(Zapowiedź z Biura \/ )(.*)', r'\2', title_of_article)
    elif category == 'wokół książek':
        related = None
    else:
        related = None
        
    
    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Sekcja': section,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Wpis dotyczy': related,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }
    all_results_biuletyn.append(dictionary_of_article)



def web_scraping_biblioteka_by_category(link):
    format_link = re.sub(r'(https\:\/\/www\.biuroliterackie\.pl\/biblioteka\/)(.*)(\/page\/)(\d*)', r'\1\2\3', link)
    for number in range(1,300):
        link = format_link+str(number)
        all_created_links_of_biblioteka.append(link)   
   
def checking_content_of_links(link): 
    html_text = requests.get(link).text
    while 'Error' in html_text:
        time.sleep(5)
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
    dictionary_of_links_with_dates = [{x:y} for x,y in list(zip(list_of_links, list_of_dates_of_publication))]
    all_list_of_links_with_dates_from_biblioteka.extend(dictionary_of_links_with_dates)


def dictionary_of_article_from_biblioteka(x):
    link = "".join([key for key,value in x.items()])
    
    #link = 'https://www.biuroliterackie.pl/biblioteka/recenzje/spalanie-grzegorza-kwiatkowskiego-2/' #recenzja
    #link = 'https://www.biuroliterackie.pl/biblioteka/cykle/7-siedmiu-moich-portowych-autorow-martyna-bulizanska-trauma/' #felieton z cyklu
    #link = 'https://www.biuroliterackie.pl/biblioteka/ksiazki/tlen/' #inna struktura danych
    #link = 'https://www.biuroliterackie.pl/biblioteka/ksiazki/hurtownia-ran-i-wiersze-ludowe/' #ksiazki (z wierszami)
    #link = 'https://www.biuroliterackie.pl/biblioteka/utwory/z-raptularza/'
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
    
    category = soup.find('span', class_='category')
    if category: 
        category = category.text
    else:
        category = None

    date_of_publication = "".join([value for key,value in x.items()])
   
    try:    
        excerpt = soup.find('p', class_='excerpt').text.strip()
    except(AttributeError, KeyError, IndexError):
        excerpt = None  
    
    author = soup.find('h4', class_='title_autor')
    if author:
        author = author.text.strip()
    else:
        author = None
          
    title_of_article = soup.find('h1', class_='title_h1')
    if title_of_article:
        title_of_article = title_of_article.text.strip()
    else:
        title_of_article = None
        
    content_of_article = soup.find('main', class_='site-main')
       
    text_of_article = [x.text.replace('\xa0','').replace('\n',' ').strip() for x in content_of_article.find_all('section', class_='single_right') if not re.findall(r'O AUTORZE', x.text)]
    if text_of_article:
        text_of_article = " ".join(text_of_article)
    else:
        text_of_article = None
          

    titles_of_poems = [x.text for x in content_of_article.find_all('h2') if not re.findall(r'(O AUTORZE)|(powiązania)|(O AUTORACH)|(Filmy autora)|(INNE GALERIE)|(inne głosy w debacie)|(Слава Україні!)|(CZYTAJ GŁOSY W DEBACIE)', x.text)]
    if titles_of_poems:
        titles_of_poems = " | ".join(titles_of_poems)
    else:
        titles_of_poems = None 
          
    about_author = content_of_article.find('p', class_='o_autorze_bio')
    if about_author:
        about_author = about_author.text
    else:
        about_author = None 
        
    try: 
        author_photo = content_of_article.find('div', class_='o_autorze_left').img['src']
    except(AttributeError, KeyError, IndexError, TypeError):
        author_photo = None 
    
    external_links = [x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'biuroliterackie|images|mail|#', x)]
    if external_links:
        external_links = ' | '.join(external_links)
    else:
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
                             'Data publikacji': date_of_publication,
                             'Tytuł czasopisma': title_of_journal,
                             'Sekcja': section,
                             'Kategoria': category,
                             'Autor': author,
                             'Nota o autorze': about_author,
                             'Zdjęcie autora': author_photo,
                             'Tytuł artykułu': title_of_article,
                             'Adnotacja': excerpt,
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


    all_results_biblioteka.append(dictionary_of_article)


def get_links_ksiazki_katalog(sitemap_link): 
    html_text = requests.get(sitemap_link).content
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc')][1:]
    all_links_from_ksiazki_katalog.extend(links)
    
def dictionary_of_ksiazki_katalog(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date_of_publication = soup.find('span', class_='data_data').text
        result = time.strptime(date_of_publication.strip(), "%d/%m/%Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = changed_date.date().strftime("%Y-%m-%d")
    except AttributeError:
        new_date = None
        
    
    title_of_book = soup.find('h4', class_='biuletyn_title')
    if title_of_book: 
        title_of_book = title_of_book.text
    
    content_of_article = soup.find('div', attrs={'id':'content'})
     
    
    note_about_book = soup.find('div', class_='ksiazka-excerpt')
    if note_about_book:
        note_about_book = note_about_book.text.strip()
    else:
        note_about_book = None

    try: 
        cover = " ".join([x['src'] for x in content_of_article.find('div', class_='col-md-4').findChildren('img')])
    except AttributeError:
        cover = None
    
    table_of_content = soup.find('div', attrs={'id':'spis-tresci'})
    if table_of_content:
        table_of_content = table_of_content.text.strip().replace('\n', ' | ')
    else:
        table_of_content = None
    
    reviews = soup.find('div', class_='row opinie-o-ksiazce lato-font')  
    if reviews:
        authors_of_reviews = [x.text for x in reviews.find_all('p', class_='autor')]
        reviews = [x.text for x in reviews.find_all('p', class_=None)]
    else:
        reviews = None
        authors_of_reviews = None
    
    
    if reviews:
        if authors_of_reviews:
            dictionary_of_reviews = {}
            for text in reviews:
                for name in authors_of_reviews:
                    dictionary_of_reviews[name] = text
    else: 
        dictionary_of_reviews = None
        
    
    
    dictionary_of_ksiazki_katalog = {'Link': link,
                         'Data publikacji': new_date,
                         'Tytuł książki': title_of_book, 
                         'Okładka': cover,
                         'Nota o książce': note_about_book,
                         'Spis treści': table_of_content,
                         'Opinie o książce': dictionary_of_reviews
                            }

    #info_field - dane ksiazki z tabeli
    info_field = soup.find_all('div', class_='info_field')
    for x in info_field:
        key = x.find('div', class_='info_field_left').text
        value = x.find('div', class_='info_field_right').text
        if not key in dictionary_of_ksiazki_katalog.keys():
            dictionary_of_ksiazki_katalog[key] = value
    
    
    all_results_ksiazki_katalog.append(dictionary_of_ksiazki_katalog)
    
    
                       
    
#%% main BIULETYN


sitemap_link_biuletyn = ['https://www.biuroliterackie.pl/biuletyn-sitemap.xml']

all_links_from_biuletyn = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_biuletyn, sitemap_link_biuletyn), total=len(sitemap_link_biuletyn)))

all_results_biuletyn = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article_from_biuletyn, all_links_from_biuletyn), total=len(all_links_from_biuletyn)))
    
df_biuletyn = pd.DataFrame(all_results_biuletyn).drop_duplicates()    
df_biuletyn = df_biuletyn.sort_values('Data publikacji', ascending=False)

#%% main Pozostałe artykuły (sekcja biblioteka)

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


all_results_biblioteka = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article_from_biblioteka, all_list_of_links_with_dates_from_biblioteka), total=len(all_list_of_links_with_dates_from_biblioteka)))

    
df_biblioteka = pd.DataFrame(all_results_biblioteka).drop_duplicates() 
df_biblioteka = df_biblioteka.sort_values('Data publikacji', ascending=False)

#merge_dataframes = pd.concat([df_biuletyn, df_biblioteka])

#%% Ksiazki katalog


all_links_from_ksiazki_katalog = [] 
get_links_ksiazki_katalog('https://www.biuroliterackie.pl/ksiazki_lista-sitemap.xml')


all_results_ksiazki_katalog = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_ksiazki_katalog, all_links_from_ksiazki_katalog), total=len(all_links_from_ksiazki_katalog)))

df_ksiazki_katalog = pd.DataFrame(all_results_ksiazki_katalog)
df_ksiazki_katalog = df_ksiazki_katalog.sort_values('Data publikacji', ascending=False)


#%% json i xlsx

with open(f'biuroliterackie_biuletyn_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results_biuletyn, f) 
with open(f'biuroliterackie_biblioteka_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results_biblioteka, f) 
with open(f'biuroliterackie_ksiazki_katalog_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results_ksiazki_katalog, f) 

with pd.ExcelWriter(f"biuroliterackie_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df_biuletyn.to_excel(writer, 'Biuletyn', index=False) 
    df_biblioteka.to_excel(writer, 'Biblioteka', index=False)
    df_ksiazki_katalog.to_excel(writer, 'Książki Katalog', index=False)
    writer.save()  

    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"biuroliterackie_{datetime.today().date()}.xlsx", f'biuroliterackie_biuletyn_{datetime.today().date()}.json', f'biuroliterackie_biblioteka_{datetime.today().date()}.json', f'biuroliterackie_ksiazki_katalog_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



#%% Notatki
#Osobno zeskrobać po przejrzeniu?: 'https://www.biuroliterackie.pl/wydarzenia-sitemap.xml' (tu trzeba trochę pomyslec nad tym co zeskrobac i jak uporzadkowac), '

#Selekcja częsci o ksiazkach z sekcji Ksiazki (dot. oferty sklepowej)
#Projekty


# 'https://www.biuroliterackie.pl/projekty-sitemap.xml'

# all_formats_links_of_biblioteka = ['https://www.biuroliterackie.pl/biblioteka/wywiady/page/1','https://www.biuroliterackie.pl/biblioteka/recenzje/page/1', 'https://www.biuroliterackie.pl/biblioteka/ksiazki/page/1', 'https://www.biuroliterackie.pl/biblioteka/utwory/page/1', 'https://www.biuroliterackie.pl/biblioteka/debaty/page/1', 'https://www.biuroliterackie.pl/biblioteka/cykle/page/1', 'https://www.biuroliterackie.pl/biblioteka/dzwieki/page/1', 'https://www.biuroliterackie.pl/biblioteka/nagrania/page/1', 'https://www.biuroliterackie.pl/biblioteka/zdjecia/page/1', 'https://www.biuroliterackie.pl/biblioteka/kartoteka_25/page/1']









