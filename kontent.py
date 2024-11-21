'''
Selenium
Strona bardzo trudna do zeskrobania
Nie ma dat publikacji
Kilka linków do numerów jest już nieaktywnych: nr 3, 4, 5, 7, 10
PROBLEM: Teksty literackie są pod jednym linkiem z komentarzami do nich. Kwestia metodologiczna, czy to rozbijamy czy nie. Na razie jest to razem. 
Skrobię strony na podstawie linku do całego czasopisma. Strona ma skomplikowaną budowę i sporo jest elementów dynamicznych. 
'''

#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

from selenium import webdriver


#%% def

def get_issues_links(archive_link):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(archive_link)
    time.sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    issues_links = [(e.a['href'], e.a.text) for e in soup.find_all('li', class_='jal rm1')]

    return issues_links
    
    
def get_article_links(link):
    
    if link.startswith('http://kontent.net.pl/czytaj'):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        driver.get(link)
        time.sleep(3)
        html = driver.page_source
        driver.quit()
        soup = BeautifulSoup(html, 'html.parser')
        
        links = [e.get('href') for e in soup.find_all('a') if e.get('href') is not None and '/czytaj/' in e.get('href')]
        full_links = [f'https://kontent.net.pl{link}' for link in links]
        articles_links.extend(full_links)

    return articles_links
    

def dictionary_of_article(issue_link):
    
    if re.search(r'https?:\/\/kontent\.net\.pl\/czytaj\/', issue_link):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        driver.get(issue_link)
        time.sleep(3)
        html = driver.page_source
        driver.quit()
        soup = BeautifulSoup(html, 'html.parser')
        issue = soup.find('header', class_='thiccOnly').text   
        sections = soup.find_all('section', id=lambda x: not x.startswith('menu') if x else False)
        
        for section in sections:     
            try:
                article_link = section.find('header').find('a')['href']
                article_link = 'https://kontent.net.pl' + article_link
            except:
                article_link = None
                
                
            # try:
            #     category = section.find('header').find('h3').text
            # except AttributeError:
            #     category = None
                
            try:
                author_of_article = section.find('strong').text
            except:
                author_of_article = None
                
            try:
                author_of_comment = [x.text for x in section.find_all('strong')][1]
            except: 
                author_of_comment = None
                
                
            try:
                title_of_article = section.find('cite').text
            except:
                title_of_article = None
                
                
            if author_of_comment != None or author_of_comment != []:
                try:
                    title_of_comment = [x.text for x in section.find_all('cite')][1]
                except:
                    title_of_comment = None
                
            
            text_of_article = " ".join([x.text for x in section.find_all('main')])
            
            dictionary_of_article = {'Link': article_link,
                                     'Numer czasopisma': issue,
                                     'Autor artykułu': author_of_article,
                                     'Tytuł artykułu': title_of_article,
                                     'Autor komentarza': author_of_comment,
                                     'Tytuł komentarza': title_of_comment,
                                     'Tekst artykułu': text_of_article,
                                     }   
            
            all_results.append(dictionary_of_article)
            

#%% main

issues_links = get_issues_links('https://kontent.net.pl/strona/89') #linki z numerami czasopisma
only_links = [t[0] for t in issues_links]


# articles_links = []    #821 artykułów 
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_article_links, only_links),total=len(only_links)))

# articles_links_unique = list(set(articles_links))   #412 bez dubletów!


all_results = []
with ThreadPoolExecutor(max_workers=4) as excecutor:
    list(tqdm(map(dictionary_of_article, only_links),total=len(only_links)))


    
with open(f'data\\kontent_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

df = pd.DataFrame(all_results).drop_duplicates()

      
with pd.ExcelWriter(f"data\\kontent_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
    
   
    



