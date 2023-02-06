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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


#%% def

def get_links_of_sitemap(sitemap_link):
    #sitemap_link = 'https://www.cultureave.com/post-sitemap2.xml'
    #sitemap_link = 'https://www.cultureave.com/post-sitemap1.xml'
    chrome_options = Options()
    chrome_options.headless = True
    driver = webdriver.Chrome("C:\\Users\\PBL_Basia\\Desktop\\ChromeDriver\\chromedriver.exe", options=chrome_options)
    driver.get(sitemap_link)
    time.sleep(8)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    links = [x.a.text for x in soup.find_all('li') if not re.match(r'cultureave\.com\/$', x.a.text) ]
    articles_links.extend(links)
    return articles_links

















def dictionary_of_article(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    author = 'Damian'
    
    date_of_publication = soup.find('li', class_='meta-date').text
    date = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s[\w]*\s[\w]*)', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    
    
    title_of_article = soup.find('h1', class_='entry-title')
    if title_of_article: 
        title_of_article = title_of_article.text
    else:
        title_of_article = None
        
    content_of_article = soup.find('section', class_='entry-content')
    if content_of_article:
        text_of_article = content_of_article.text.strip().replace('\n', ' ').replace('\t', ' ').replace('  ', ' ')
    else:
        text_of_article = None     
              
    category = soup.find('ul', class_='post-categories').findChildren('li')
    if category != '':
        category = " | ".join([x.text for x in category])
    else:
        category = None
    
    tags = soup.find('section', class_='post-tags').findChildren('li')
    if tags != '':
        tags = " | ".join([x.text for x in tags])
    else:
        tags = None
    
    external_links = [x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'kochampolskiekino|images|mail|#', x)]
    if external_links: 
        external_links = ' | '.join(external_links)
    else:
        external_links = None    
        
    photos_links = [x['src'] for x in content_of_article.find_all('img')]  
    if photos_links != '':
        photos_links = ' | '.join(photos_links)
    else:
        photos_links = None
        
        
    
    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                            }
    
    all_results.append(dictionary_of_article)

















#%% main
sitemap_links = ['https://www.cultureave.com/post-sitemap2.xml', 'https://www.cultureave.com/post-sitemap1.xml']

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap, sitemap_links),total=len(sitemap_links)))


articles_links = articles_links.remove('https://www.cultureave.com/')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))






with open(f'cultureave_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        


df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)


with pd.ExcelWriter(f"cultureave_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"cultureave_{datetime.today().date()}.xlsx", f'cultureave_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  


















