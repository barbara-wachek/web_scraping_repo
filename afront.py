#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_short, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def
def afront_web_scraping_sitemap_pages(link): 
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_pages = [e.text for e in soup.find_all('loc')]
    return links_pages 

def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while '429 Too Many Requests' in html_text:
        time.sleep(5)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('time', class_= re.compile(r"entry-date")).text
    new_date = date_change_format_short(date_of_publication)
    text_of_article = soup.find('div', class_='entry-content single-content')
    article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    author = " | ".join([x.text for x in text_of_article.find_all('p', attrs={'style':'text-align: right;'})])
    title_of_article = soup.find('h1', class_='entry-title').text
    tags = ''.join([x.text.replace('\n','').strip() for x in soup.find_all('span', class_='category-links term-links category-style-normal')])
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'afront\.org', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')][1:])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    dictionary_of_article = {"Link": article_link, 
                             "Data publikacji": new_date,
                             "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                             "Tekst artykułu": article,
                             "Autor": author,
                             "Tagi": tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }
    
    all_results.append(dictionary_of_article)    
    
        
def extras_content_authors(notes_about_authors):
    html_text = requests.get(notes_about_authors).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    text_of_extras = soup.find('div', class_='entry-content single-content')
    notes = [x.text.replace('\xa0',' ') for x in text_of_extras.find_all('p')][1:]
    authors = [x.text.replace('\xa0','') for x in text_of_extras.find_all('strong')][1:]
    new_list_of_authors = [x for x in authors if len(x) > 8 if x != 'البائدة المدن / ']
    new_list = list(zip(new_list_of_authors, notes)) 
  
    for element in new_list:
        dictionary_of_extras = {}
        dictionary_of_extras['Osoba'] = element[0]
        dictionary_of_extras['Biogram'] = element[1]
        
        all_dictionaries_of_extras.append(dictionary_of_extras)
        
    return all_dictionaries_of_extras


#%%main 
articles_links = get_links('https://afront.org.pl/wp-sitemap-posts-post-1.xml')
extras_pages_links = afront_web_scraping_sitemap_pages('https://afront.org.pl/wp-sitemap-posts-page-1.xml')

all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

all_dictionaries_of_extras = []
biograms = extras_content_authors('https://afront.org.pl/nasi-autorzy/')
    
with open(f'afront_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 
with open(f'afront_extras_biograms_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(biograms, f)             
    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

df_extras_authors = pd.DataFrame(biograms)
df_extras_all_pages = pd.DataFrame(extras_pages_links)


with pd.ExcelWriter(f"afront_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    df_extras_authors.to_excel(writer, 'Biograms', index=False, encoding='utf-8')   
    df_extras_all_pages.to_excel(writer, 'Subpages', index=False, encoding='utf-8')  
    writer.save()    


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"afront_{datetime.today().date()}.xlsx", f'afront_{datetime.today().date()}.json', f'afront_extras_biograms_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



#Naprawić kod 
    
#Nie zapisywac w excelu - skoro gubia sie dane
#jak zachowac tabele, zeby miec dostep do pelnych linkow
#przepisać wszystkie funkcje zgodnie z tymi wytycznymi 
#trudny serwis = Andrzej Pilipiuk    
    
    
#limit słów w komorce Excela = 32 767 characters
#limit słów w komórce Google Sheet = 50 000 characters   

#jako rozwiązanie dodałam atrybut w pd.ExcelWriter - options={'strings_to_urls': False} zgodnie z tą radą: 
    #https://stackoverflow.com/questions/35440528/how-to-save-in-xlsx-long-url-in-cell-using-pandas 
    
    
    
    
    
    
    