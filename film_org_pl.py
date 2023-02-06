#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
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
    chrome_options = Options()
    chrome_options.headless = True
    driver = webdriver.Chrome("C:\\Users\\PBL_Basia\\Desktop\\ChromeDriver\\chromedriver.exe", options=chrome_options)
    driver.get(sitemap_link)
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    links = [x.text for x in soup.find_all('td', class_='left')]
    articles_links.extend(links)
    return articles_links
    

def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    author_and_date = soup.find('div', class_='single')

    if author_and_date: 
        author = " | ".join([x.text for x in author_and_date.findChildren('p', class_=None)])
    else:
        author = None
    
    new_date = None    
    if author_and_date:
        date_of_publication = [x.text for x in author_and_date.findChildren('p', class_='pink--date')]
        if date_of_publication != '':
            date_of_publication = "".join(date_of_publication)
            lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
            
            for k, v in lookup_table.items():
                date_of_publication = date_of_publication.replace(k, v)
                
            result = time.strptime(date_of_publication, "%d %m %Y")
            changed_date = datetime.fromtimestamp(mktime(result))   
            new_date = format(changed_date.date())
        else:
            new_date = None
        
        
    title_of_article = soup.find('h1')
    if title_of_article:
        title_of_article = title_of_article.text.strip()
    else:
        title_of_article = None

    subtitle = soup.find('div', class_='single__page__forecast')
    if subtitle:
        subtitle = subtitle.text.strip()
    else:
        subtitle = None
    
    content_of_article = soup.find('div', class_='js-single-container')
    if content_of_article:
        text_of_article = " | ".join([x.text for x in content_of_article.findChildren('p')])
    else:
        text_of_article = None
        
    category = soup.find('div', class_='category__box')
    if category:
        category = category.text.strip()
    else:
        category = None
    
    
    
    about_author = soup.find('div', class_='author__description')
    if about_author:
        about_author = "".join([x.text for x in soup.find('div', class_='author__description').findChildren('p', class_=None)])
    else: 
        about_author = None
    
    tags = soup.find('div', class_='single__page__tags')
    if tags:
        tags = " | ".join([x.text for x in soup.find('div', class_='single__page__tags').findChildren('a')])
    else:
        tags = None

       

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Podtytuł': subtitle,
                             'Kategoria': category, 
                             'Tekst artykułu': text_of_article,
                             'Nota o autorze': about_author,  
                             'Tagi': tags}
        

    all_results.append(dictionary_of_article)

#%% main

sitemap_links = ['https://film.org.pl/post-sitemap.xml', 'https://film.org.pl/post-sitemap2.xml']

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap, sitemap_links),total=len(sitemap_links)))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    
 
df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
    
 
with open(f'film_org_pl_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)           
 
    
with pd.ExcelWriter(f"film_org_pl_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"film_org_pl_{datetime.today().date()}.xlsx", f'film_org_pl_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()   
    
 
    












    
    
    