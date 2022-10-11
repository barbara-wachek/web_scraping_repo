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
def get_audycjekulturalne_posts_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links_pages = [e.text for e in soup.find_all('loc')]
    all_posts_links.extend(links_pages)
    return all_posts_links

def dictionary_of_article(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')

    date_of_publication = re.sub(r'(\d{4}-\d{2}-\d{2})(T.*)', r'\1', soup.find('time', class_='entry-date published')['datetime'])
    author = soup.find('span', class_='author vcard').text.strip()
    
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except (AttributeError, KeyError, IndexError):
        title_of_article = None
        
    content_of_article = soup.find('div', class_='entry-content')
    text_of_article = " ".join([x.text.replace('\n','') for x in content_of_article.find_all('p')])
    tags = soup.find('span', class_='tags-links').text.replace('Tagi: ','').replace(',', ' |')
    

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'audycjekulturalne|images', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
        
    try: 
        program_link = [e for e in [x['href'] for x in content_of_article.find_all('a')] if '.mp3' in e][0]
    except (AttributeError, KeyError, IndexError):
        program_link = None
   
    try: 
        transcription_pdf = " | ".join([e for e in [x['href'] for x in content_of_article.find_all('a')] if '.pdf' in e])
    except (AttributeError, KeyError, IndexError):
        transcription_pdf = None

    dictionary_of_article = {'Link': link,
                             'Data publikacji': date_of_publication, 
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Transkrypcja': transcription_pdf,
                             'Link do pliku audio': program_link,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }


    all_results.append(dictionary_of_article)
    

#%%main

sitemap_posts_links = ['https://audycjekulturalne.pl/post-sitemap.xml',
                       'https://audycjekulturalne.pl/post-sitemap2.xml']

all_posts_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_audycjekulturalne_posts_links, sitemap_posts_links),total=len(sitemap_posts_links)))   
    
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_posts_links), total=len(all_posts_links)))

with open(f'audycjekulturalne_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results)
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"audycjekulturalne_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   
      
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"audycjekulturalne_{datetime.today().date()}.xlsx", f'audycjekulturalne_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  













    
    
 
    
    
