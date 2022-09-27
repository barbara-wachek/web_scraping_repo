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

#%%

# link = 'https://czaskultury.pl/archiwum-tekstow/' == 'https://czaskultury.pl/archiwum-tekstow/page/1/'

# https://czaskultury.pl/wydarzenia/
# https://czaskultury.pl/reading-book-sitemap.xml #?
# https://czaskultury.pl/reading-book-sitemap2.xml

# https://czaskultury.pl/feuilleton-sitemap.xml

# sitemap = 'https://czaskultury.pl/sitemap_index.xml'


def number_of_pages_in_archive_of_texts(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    number_of_pages_in_archive_of_texts = soup.find_all('a', class_='page-numbers')[-2].text
    return number_of_pages_in_archive_of_texts

def czas_kultury_get_links_from_archive(digit_from_range):
    format_url = 'https://czaskultury.pl/archiwum-tekstow/page/'
    working_url = f'{format_url}{digit_from_range}'
    links_of_archive_pages.append(working_url)
    return links_of_archive_pages

def czas_kultury_web_scraping_links_from_archive(link_of_archive_page):
    html_text = requests.get(link_of_archive_page).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('div', class_='post-box-component')]
    all_archive_links.extend(links)
    return all_archive_links

def links_of_archive_years(link):
    link = 'https://czaskultury.pl/archiwum-numerow/'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text)
    links = [x['href'] for x in soup.find_all('a', class_='font-weight-normal h3')]
    return links
    
def czas_kultury_get_links_from_article_archive(link): 
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    all_articles = [x.a['href'] for x in soup.find_all('div', class_="post-box-component")]
    all_articles_from_archive.extend(all_articles)
    return all_articles_from_archive

def dictionary_of_article(link):
    #link = 'https://czaskultury.pl/artykul/pijesz-ty-pije-ja/' #archiwum numerów
    #link = 'https://czaskultury.pl/felietony/sacrum-w-niewoli/' #archiwum tekstów
    #link = 'https://czaskultury.pl/artykul/szalenstwo-pszczol/' #archiwum numeró♥w
    #link = 'https://czaskultury.pl/czytanki/dystopia-z-osmiornica-w-tle/'
    #link = 'https://czaskultury.pl/czytanki/kara-walker-i-prawo-do-mowienia/'
    #link = 'https://czaskultury.pl/czytanki/przez-ucho-do-plocka/'
    #link = 'https://czaskultury.pl/czytanki/przestrzen-teatralna-w-pradze/'
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
        date_of_publication = soup.find('div', class_='col-xl-1 col-md-2 col-6 order-1 order-md-0 mb-4 mb-md-0 position-md-sticky top30').text.strip() 
        if 'Archiwum' in date_of_publication: 
            date_of_publication = date_of_publication.replace('Archiwum', '').replace('ARCHIWUM', '').replace('\n', '').strip()
            result = time.strptime(date_of_publication, "%d.%m.%Y")
            changed_date = datetime.fromtimestamp(mktime(result))   
            new_date = format(changed_date.date())
        elif "ARCHIWUM" in date_of_publication: 
            date_of_publication = date_of_publication.replace('Archiwum', '').replace('ARCHIWUM', '').replace('\n', '').strip()
            result = time.strptime(date_of_publication, "%d.%m.%Y")
            changed_date = datetime.fromtimestamp(mktime(result))   
            new_date = format(changed_date.date())
        else:
            year_and_issue = re.sub(r'(Nr\s)?(\d{1,2}\/\d{4})(\s*)(.*)', r'\2', date_of_publication) if "Nr" or "/" in date_of_publication else None
            new_date = None
    except (AttributeError, KeyError, IndexError):
        new_date = None
    except ValueError:
        date_of_publication = date_of_publication.replace('Archiwum', '').replace('ARCHIWUM', '').replace('\n', '').strip()
        lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "Lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
        for k, v in lookup_table.items():
            date_of_publication = date_of_publication.replace(k, v)
        result = time.strptime(date_of_publication, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())
    
    content_of_article = soup.find('div', class_='row align-items-start')
    text_of_article = soup.find('div', class_='paragraph fs-16').text.replace('\xa0', ' ').replace('\n', ' ').strip()
    title_of_article = soup.find('h1', class_='mb-0').text.strip().replace('\xa0', ' ')
    author = ' | '.join([x.span.text for x in soup.find_all('div', class_='post-box-component__authors separated-comma my-1')])
 
    try:
        additional_info = '| '.join([x.text.replace('\r\n', " | ").replace('\n', " ") for x in soup.find_all('p', class_='h3')])
    except (AttributeError, KeyError, IndexError):
        additional_info = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(czaskultury)|(mailto)|(/)', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None

    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
       
        

    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Numer/rok czasopisma': year_and_issue if new_date == None else None,
                             'Dodatkowe informacje': additional_info,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }


    all_results.append(dictionary_of_article)    


#%% main 

#Linki z Archiwum tekstów:
number_of_pages_in_archive_of_texts =  number_of_pages_in_archive_of_texts('https://czaskultury.pl/archiwum-tekstow/page/6/')

links_of_archive_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_get_links_from_archive, range(1, int(number_of_pages_in_archive_of_texts)+1)), total=len(range(1, int(number_of_pages_in_archive_of_texts)+1))))

all_archive_links = []  
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_web_scraping_links_from_archive, links_of_archive_pages), total=len(links_of_archive_pages)))


#Linki z Archiwum numerów:

links_of_archive_years = links_of_archive_years('https://czaskultury.pl/archiwum-numerow/')

all_articles_from_archive = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_get_links_from_article_archive, links_of_archive_years), total=len(links_of_archive_years)))
    
#Wywalenie ewentualnych duplikatów z listy linków z archiwum artykułów:
all_articles_from_archive = list(set(all_articles_from_archive))

#Wywalenie ewentualnych duplikatów z listy linków z archiwum artykułów:
all_archive_links = list(set(all_archive_links))   


#Dodanie obu list, aby otrzymać listę wszystkich linków ze strony: 
all_articles_links = all_archive_links + all_articles_from_archive


#Scrapowanie artykułów z listy linków

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links),total=len(all_articles_links)))   


with open(f'czas_kultury_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results).drop_duplicates()

#Zrobic dwa osobne df dla Archiwum tekstów i Archiwum numerów - tam gdzie nie ma daty to ARchiwum numerów i potem zapisać je w osobnych arkuszach Excela
df_archiwum_tekstow = df[df['Data publikacji'].notna()]
df_archiwum_tekstow["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df_archiwum_tekstow  = df.sort_values('Data publikacji', ascending=False)


df_archiwum_numerow = df[df['Data publikacji'].isna()]


#Uzupełnienie raportu
#report = ScrapingReport('https://www.pilipiuk.com', len(df), df['Data publikacji'].iloc[0])    
#report.create_scraping_report()


with pd.ExcelWriter(f"czas_kultury_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df_archiwum_numerow.to_excel(writer, 'Archiwum numerów', index=False, encoding='utf-8')   
    df_archiwum_tekstow.to_excel(writer, 'Archiwum tekstów' index=False, encoding='utf-8')
    writer.save()  




#Dodać opcję sortowania w archiwum numerów po numerze - czyli ujednolicić zapis 
#wywalić takie rzeczy https://czaskultury.pl/czytanki/kara-walker-i-prawo-do-mowienia/ (ma inny format daty)
#https://czaskultury.pl/czytanki/przestrzen-teatralna-w-pradze/
#https://czaskultury.pl/czytanki/dystopia-z-osmiornica-w-tle/
#https://czaskultury.pl/czytanki/tanczcie-inaczej-jestesmy-zgubieni/
#https://czaskultury.pl/czytanki/przez-ucho-do-plocka/












#Sprawdz, czy ktorykolwiek z elementow all_archive_links zawiera fragment tekstu, ktory nazywa sie pijesz-ty-pije-ja



re.findall(r'pijesz\-ty\-pije\-ja', all_archive_links)

[re.findall(r'pijesz\-ty\-pije\-ja',x) for x in all_archive_links]


for element in all_archive_links:
    if re.findall(r'pijesz\-ty\-pije\-ja', element):
        print(True)

[x for x in all_archive_links if 'pijesz-ty-pije-ja' in x]

    
'pijesz-ty-pije-ja'in'https://czaskultury.pl/artykul/pijesz-ty-pije-ja/' 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    