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
                
            if author_of_comment == author_of_article or title_of_comment == title_of_article:
                author_of_comment = None
                title_of_comment = None
            
            
            text = " ".join([x.text for x in section.find_all('main')])
            
            try:
                span = re.search(f'{author_of_comment}', text).span(0)[0]
                text_of_article = text[0:span]
                text_of_comment = text[span:]
            except:
                text_of_article = text
                text_of_comment = None

            
            dictionary_of_article = {'Link': article_link,
                                     'Numer czasopisma': issue,
                                     'Autor artykułu': author_of_article,
                                     'Tytuł artykułu': title_of_article,
                                     'Autor komentarza': author_of_comment,
                                     'Tytuł komentarza': title_of_comment,
                                     'Tekst artykułu': text_of_article,
                                     'Tekst komentarza': text_of_comment
                                     }   
            
            all_results.append(dictionary_of_article)
            

#%% main

issues_links = get_issues_links('https://kontent.net.pl/strona/89') #linki z numerami czasopisma
only_links = [t[0] for t in issues_links]


all_results = []
with ThreadPoolExecutor(max_workers=10) as excecutor:
    list(tqdm(map(dictionary_of_article, only_links),total=len(only_links)))
    

df = pd.DataFrame(all_results).drop_duplicates()
#Rozbicie podwójnych rekordów: tekst + komentarz na dwa osobne rekordy


df_only_articles = df.drop(columns=['Autor komentarza', 'Tytuł komentarza', 'Tekst komentarza'])
df_only_articles = df_only_articles.loc[df_only_articles['Tytuł artykułu'].notna()]
df_only_articles.columns
df_only_articles = df_only_articles.rename(columns={'Autor artykułu': 'Autor'})


#Wyrzucenie pustych wartosci (niektóre artykuły nie miały komentarzy)
df_only_comments = df.drop(columns=['Autor artykułu', 'Tytuł artykułu', 'Tekst artykułu'])
df_only_comments = df_only_comments.loc[df_only_comments['Tytuł komentarza'].notna()]
df_only_comments = df_only_comments.rename(columns={'Autor komentarza':'Autor', 'Tytuł komentarza': 'Tytuł artykułu', 'Tekst komentarza':'Tekst artykułu'})
df_only_comments['Uwagi'] = 'komentarz'


#połączenie w 1 dataframe
df_concat = pd.concat([df_only_articles, df_only_comments])
df_concat = df_concat.replace({np.nan: None})


list_of_dicts = df_concat.to_dict(orient='records')

with open(f'data\\kontent_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(list_of_dicts, f, ensure_ascii=False)   
      
with pd.ExcelWriter(f"data\\kontent_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df_concat.to_excel(writer, 'Posts', index=False)   
    
   
    



