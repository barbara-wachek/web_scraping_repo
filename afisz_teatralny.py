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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%def
def afisz_teatralny_web_scraping(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links  

def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.findall(r'pt\-page-', link)]
    articles_links.extend(sitemap_links) 


def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = re.sub(r'(http:\/\/www\.afiszteatralny\.pl\/)(\d{4})(\/)(\d{2})(\/[\w\.\-]*)', r'\2-\4', article_link)
    text_of_article = soup.find('div', class_='post-entry')
    title_of_article = soup.find('div', class_='post-header').h1.text.strip()
    tags = " ".join([x.text.replace("\n", " ").strip() for x in soup.find_all('div', class_='entry-tags gray-2-secondary')]).replace('Tags:', '').replace(',', ' |')
    article = text_of_article.text.strip().replace('\n', '')
    
    if re.findall(r'\sreż\.',title_of_article) != []:
        director = re.sub(r'(.*)(\sreż\.)(.*)', r'\3', title_of_article).strip() 
        spectacle = re.sub(r'(.*)(\,\sreż\.)(.*)', r'\1', title_of_article)
    else:
        director = None
        spectacle = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogspot|jpg|blogger|afiszteatralny', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
      
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None   
        
        
    dictionary_of_article = {'Link' : article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': 'Agnieszka Kobroń',
                             'Tagi': tags if tags != '' else None,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': article,
                             'Tytuł spektaklu': spectacle,
                             'Reżyser': director,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}    
     

    all_results.append(dictionary_of_article)    



#%%main

sitemap_links = afisz_teatralny_web_scraping('http://www.afiszteatralny.pl/sitemap.xml')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
    
all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   
    
with open(f'afisz_teatralny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   
     

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)


path = f"afisz_teatralny_{datetime.today().date()}.xlsx"

with pd.ExcelWriter(path, engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')    
    writer.save()       
    
    
    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"afisz_teatralny_{datetime.today().date()}.xlsx", f'afisz_teatralny_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  
    
    
    
    
    
    
    