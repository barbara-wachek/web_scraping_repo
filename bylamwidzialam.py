#%% import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%% def

def get_links_from_sitemap(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc')]
    all_links.extend(links)
    
def get_articles_links_from_all_links(link):
    if re.match(r'https\:\/\/bylamwidzialam\.pl\/\d{4}\/\d{2}\/\d{2}.*', link):
        articles_links.append(link)
    
    


#%% main 

all_links = []
get_links_from_sitemap('https://bylamwidzialam.pl/sitemap.xml')


articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_articles_links_from_all_links, all_links),total=len(all_links))) 

   
all_results = []  
#Przy zastosowaniu wielowatkowosci niektore artykuly sie nie doczytywaly i pola w tabeli byly puste. ARtykulow jest tak malo, ze mozna to rozwiazac zwykla pętlą
for link in tqdm(articles_links): 
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(3)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = re.sub(r'(https\:\/\/bylamwidzialam\.pl\/)(\d{4})(\/)(\d{2})(\/)(\d{2}).*', r'\2-\4-\6', link)
    
    
    content_of_article = soup.find('div', class_='entry-content')
    
    author = 'Monika Krawczak' 
    
    title_of_article = soup.find('h1', class_='entry-title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
    
    
    if content_of_article: 
        text_of_article = [x.text for x in content_of_article.find_all('p')]
        if text_of_article != []: 
            text_of_article = " | ".join(text_of_article).replace('\n', ' ')
        else:
            text_of_article = None
    else:
        text_of_article = None
    
    category = [x.text for x in soup.find_all('a', attrs={'rel':'category tag'})]
    if category != []:
        category = " | ".join(category)
    else:
        category = None
        
    tags = soup.find('span', class_='tags-links')
    if tags: 
        tags = " | ".join([x.text for x in soup.find('span', class_='tags-links').findChildren('a')])
    else:
        tags = None
    
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'bylamwidzialam', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
      
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None   
        
    if content_of_article:
        videos = [x['src'] for x in content_of_article.find_all('iframe')]
        if videos != []:
            videos = True
        else:
            videos = False
    else:
        videos = False
        
        
    dictionary_of_article = {'Link' : link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria' : category,
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             'Zdjęcia/Grafika': True if photos_links else False,
                             'Filmy': videos
                             }    
     

    all_results.append(dictionary_of_article)      
    
    

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)    
    
    
with open(f'bylam_widzialam_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   
     

with pd.ExcelWriter(f"bylam_widzialam_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')    
    writer.save()       
    
    
    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"bylam_widzialam_{datetime.today().date()}.xlsx", f'bylam_widzialam_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    