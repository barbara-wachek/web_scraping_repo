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

#%%def

def get_artpapier_issue_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    all_issues = soup.select('#archiwum a')
    format_url = 'http://artpapier.com/'
    links = [x['href'] for x in  all_issues]
    proper_links = [format_url+e for e in links]
    return proper_links

def get_links_of_category_issue(link):    
    literatura_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1literatura\3', link)
    idee_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1idee\3', link)
    prezentacje_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1prezentacje\3', link)
    poezja_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1poezja\3', link)
    film_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1film\3', link)
    komiks_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1komiks\3', link)
    muzyka_category_page = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(glowna)(&wydanie=\d+)', r'\1muzyka\3', link)
    
    list_of_category_pages_links = [literatura_category_page, idee_category_page, prezentacje_category_page, poezja_category_page, film_category_page, komiks_category_page, muzyka_category_page]
    all_category_pages_links.extend(list_of_category_pages_links)
    
    return all_category_pages_links
    
def get_links_of_article_from_category_issue(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.a['href'] for x in soup.find_all('div', class_='artykulTitle')]
    format_url = 'http://artpapier.com/'
    proper_links = [format_url+e for e in links]
    all_links.extend(proper_links)


def dictionary_of_article(link):
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'html5lib')

#print(soup.original_encoding) Nie ma encodingu
#POMOGLA zmiana .text na .content przy requests.get(link).content
#First of all, don't use response.text! It is not BeautifulSoup at fault here, you are re-encoding a Mojibake. The requests library will default to Latin-1 encoding for text/* content types when the server doesn't explicitly specify an encoding, because the HTTP standard states that that is the default.

    number_of_issue = soup.find('p', attrs = {'id': 'nrWydania'}).text  
    author = " ".join([x.text.replace(',','').strip() for x in soup.find_all('p', class_='artAutor')])
    title_of_article = soup.find('p', class_='artTytul').text 
    
    try:
        number_of_category = re.sub(r'(http\:\/\/artpapier\.com\/index\.php\?page=)(artykul&n?u?l?l?&?wydanie=)(\d+&artykul=\d+&kat=)(\d)', r'\4', link)
        # dict_of_category = {'1': 'literatura', '2': 'sztuka', '3':'film', '4': 'teatr', '15': 'idee', '18':'komiks', '13': 'prezentacje', '17': 'poezja'}
        # for key, value in dict_of_category.items():
        #     category = number_of_category.replace(key, value)  #Dlaczego to nie działa?

        if number_of_category == '18':
            category = 'komiks'
        elif number_of_category == '17':
            category = 'poezja'
        elif number_of_category == '1':
            category = 'literatura'
        elif number_of_category == '2':
            category = 'sztuka'
        elif number_of_category == '4':
            category = 'teatr'
        elif number_of_category == '15':
            category = 'idee'
        elif number_of_category == '13':
            category = 'prezentacje'
        elif number_of_category == '5':
            category = 'muzyka'
        elif number_of_category == '3':
            category = 'film'
        else:
            category = None
    except(AttributeError, KeyError, IndexError):
        category = None
    
    
    content_of_article = soup.find('div', attrs={'id':'artykulContent'})
    text_of_article = soup.find('div', attrs={'id':'artTxt'}).text.strip().replace('\n', '')
    additional_information = " ".join([x.text.strip() for x in soup.find_all('div', attrs={'id':'artStopka'})]) 
    title_of_journal = 'ArtPapier' 
   
    try:
        author_of_related_work = " ".join(re.findall(r'(^[\p{L}\s\-\.\'\,]*)', additional_information))
    except (AttributeError, KeyError, IndexError):
        author_of_related_work = None
        
    try:
        title_of_related_work = " ".join(re.findall(r'(?<=\:\s)?(„.*”\)?)', additional_information))
    except (AttributeError, KeyError, IndexError):
        title_of_related_work = None
    

    try:
        external_links = ' | '.join([x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'artpapier|images|mail|index', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
     
    try: 
        photos_links = [x['src'] for x in content_of_article.find_all('img')]
        photos_links = " | ".join(['http://artpapier.com/'+x for x in photos_links])
    except (AttributeError, KeyError, IndexError):
        photos_links = None
       
     
    dictionary_of_article = {'Link': link,
                             'Tytuł czasopisma': title_of_journal,
                             'Numer/rok czasopisma': number_of_issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': category,
                             'Opis dzieła': additional_information,
                             'Autor dzieła': author_of_related_work if category != 'prezentacje' else None,
                             'Tytuł dzieła': title_of_related_work if category != 'prezentacje' else None, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }
     
     
    all_results.append(dictionary_of_article)   
    


#%% main

issue_links = get_artpapier_issue_links('http://artpapier.com/index.php?page=archiwum&wydanie=448')

#Generowanie linków na podstawie ustalonego formatu - niektóre linki nie będą mieć zawartosci, bo w danym numerze nie było artykułow z tej kategorii. Potem w kolejnej funkcji po prostu z pustej strony nie będzie można pobrać linków do artykułów
all_category_pages_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_category_issue, issue_links),total=len(issue_links)))
    
all_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_article_from_category_issue, all_category_pages_links),total=len(all_category_pages_links)))
    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_links), total=len(all_links)))


with open(f'artpapier_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results).drop_duplicates()  #Nie ma sortowania, bo brak daty publikacji. Potem mozna zrobic sortowanie po wyjeciu info z kolumny Numer/rok czasopisma

with pd.ExcelWriter(f"artpapier_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Articles', index=False, encoding='utf-8')   
    writer.save()  


    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"artpapier_{datetime.today().date()}.xlsx", f'artpapier_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

    

   







