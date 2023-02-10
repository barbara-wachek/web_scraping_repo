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
#from pydrive.auth import GoogleAuth
#from pydrive.drive import GoogleDrive
import time


from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.by import By


# from requests_html import HTMLSession


from helium import *
from time import mktime
from selenium.webdriver.chrome.options import Options



from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


#biblioteka 
import requests_html

#%% def

def get_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [x.text for x in soup.find_all('loc')]
    
    return sitemap_links


def get_article_links_from_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc')]
    all_articles_links.extend(links)
    return all_articles_links


def dictionary_of_article(link): 
    
    # link = 'https://culture.pl/pl/artykul/10-architektonicznych-atrakcji-kielecczyzny'
    # link = 'https://culture.pl/pl/artykul/niesiemy-dla-was-bombe-polskie-manifesty-filmowe'
    link = 'https://culture.pl/pl/artykul/niesiemy-dla-was-bombe-polskie-manifesty-filmowe'
    
    
# all_results = []    
# for link in tqdm(all_articles_links):
    
    chrome_options = Options()
    chrome_options.headless = True
    
    driver = webdriver.Chrome("C:\\Users\\PBL_Basia\\Desktop\\ChromeDriver\\chromedriver.exe", options=chrome_options)
    driver.get(link)
    time.sleep(4)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    test_1 = str(soup)
    
        
    html_text = requests.get('https://api.culture.pl/en/api/node/article').text
    soup = BeautifulSoup(html_text, 'lxml')
    test_1 = str(soup)
    
    
    
    
#2023-02-10    
    
    from requests_html import AsyncHTMLSession
    asession = AsyncHTMLSession()
    
    r = asession.get('https://culture.pl/pl/artykul/niesiemy-dla-was-bombe-polskie-manifesty-filmowe')
    await r.html.arender()
    
    
    
from requests_html import AsyncHTMLSession

async def get_website(url: str):
   
    asession = AsyncHTMLSession() 

    r = await asession.get(url)

    await r.html.arender(sleep = 10) # sleeping is optional but do it just in case

    html = r.html.raw_html # this can be returned as your result

    await asession.close() # this part is important otherwise the Unwanted Kill.Chrome Error can Occur 

    return html
    


test_2 = await get_website('https://culture.pl/pl/artykul/niesiemy-dla-was-bombe-polskie-manifesty-filmowe')
test_3 = str(test_2)

    
    
    #headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    # browser = start_chrome(link, headless=True)
    # time.sleep(3)

    # soup = BeautifulSoup(browser.page_source, 'lxml')
    
    
    date_of_publication = ''
    date_of_publication = soup.find('div', class_='published')
    if date_of_publication: 
        date_of_publication = date_of_publication.text 
        date = re.sub(r'(Opublikowany\:\s)(\d{1,2}\s)(\p{L}*)(\s)(\d{4})', r'\2\3\4\5', date_of_publication).strip()
        lookup_table = {"sty": "01", "lut": "02", "mar": "03", "kwi": "04", "maj": "05", "czer": "06", "lip": "07", "sier": "08", "wrz": "09", "paź": "10", "lis": "11", "gru": "12"}
        for k, v in lookup_table.items(): 
            date = date.replace(k, v)
            
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())
           
    else:
        new_date = None #wpisy o tworcach i dzielach i nie maja dat
    
        
    author = soup.find('div', class_='author-name')
    if author:
        author = author.text
        author = re.sub(r'(Autor|Autorzy)(\:\s)(.*)', r'\3', author)
        
    else:
        author = None
    
    
    title_of_article = soup.find('h1', class_='title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
           
    category = re.sub(r'(https?\:\/\/culture\.pl\/)(pl\/)?(\w*)(\/.*)', r'\3', link)
    content_of_article = soup.find('div', class_='article-content')

    about_author = soup.findChildren('div', class_='group-text')
    if about_author:
        about_author = " | ".join([x.find('div', class_='description').text for x in about_author if x.find('div', class_='description')])
    else:
        about_author = None
        
    
        
    text_of_article = [x.p.text.replace('\n', ' ') for x in content_of_article.find_all('div', class_='content')]
    if text_of_article:
        text_of_article = " | ".join(text_of_article).strip()
    else:
        text_of_article = None

    tags = soup.find('div', class_='topic')
    if tags: 
        tags = ' | '.join([x.text.replace('#','') for x in soup.find('div', class_='topic')])
    else:
        tags = None
    
    
    external_links = [x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(culture)|(^\/pl\/.*)|(^\/\s?$)', x)]
    if external_links != []:
        external_links = ' | '.join(external_links)
    else:
        external_links = None
        

    photos_links_with_description = [{x['data-src']:x['title'].replace('\xa0', '').replace('\u200b', '')} for x in content_of_article.find_all('img')]
    if photos_links_with_description == []:
         photos_links_with_description = None

       
        
    
    dictionary_of_article = {'Link': link, 
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć z podpisami': photos_links_with_description
                             }

    all_results.append(dictionary_of_article)
    




#%%main

sitemap_links = get_sitemap_links('https://culture.pl/pl/sitemap.xml')    

all_articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links_from_sitemap_links, sitemap_links), total=len(sitemap_links)))       


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links), total=len(all_articles_links)))   











