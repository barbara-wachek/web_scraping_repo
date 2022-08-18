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



#%%def
def krytycznym_okiem_web_scraping_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)
    
def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_article = {}
    
    author = "Jarosław Czechowicz"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    author_of_book = re.findall(r'(?<=[\"|\”]\s)(.*\s*\.*\s*?\.*?)', title_of_article)  
    title_of_book = re.findall(r'[\"\„].*[\"\”](?=\s.*)', title_of_article)
    date_of_publication = soup.find('h2', class_='date-header').text
    texts_of_article = soup.find_all('div', class_='post-body entry-content')
    tags = soup.find_all('span', class_='post-labels')     
    
    
    for element in texts_of_article:
        try:
            article = element.text.strip()
            
            dictionary_of_article['Link'] = article_link
            dictionary_of_article['Autor'] = author
            dictionary_of_article['Tytuł artykułu'] = title_of_article
            dictionary_of_article['Data publikacji'] = date_of_publication
           
            dictionary_of_article['Autor książki'] = author_of_book[0]
            dictionary_of_article['Tytuł książki'] = title_of_book[0]
            dictionary_of_article['Tekst artykułu'] = article.replace('\n', ' ')
            
            dictionary_of_article['Odautorski tytuł recenzji'] = re.findall(r'(?<=Tytuł recenzji:\s).*\s?.*?(?=\p{Lu}|\n)', article)[0].replace('\n', ' ').strip() 
            dictionary_of_article['Data wydania książki'] = re.findall(r'(?<=Data wydania:\s).*(?=\n)', article)[0]
            dictionary_of_article['Wydawnictwo'] = re.findall(r'(?<=Wydawca:\s).*\s?.*?(?=\n|D)', article)[0].replace('\n', ' ').strip()
            
            dictionary_of_article['Tagi'] = [tag.text.replace('Etykiety:', '').strip().replace('\n', ' ') for tag in tags][0]
            
            
        except AttributeError:
            pass 
        except IndexError:   
            pass
        all_results.append(dictionary_of_article)
        

#%% main
sitemap_links = krytycznym_okiem_web_scraping_sitemap('http://krytycznymokiem.blogspot.com/sitemap.xml')    
articles_links = []    

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
   
all_results = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'krytycznym_okiem_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)   


df = pd.DataFrame(all_results)
df = df.drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)
df.to_excel(f"krytycznym_okiem_{datetime.today().date()}.xlsx", encoding='utf-8', index=False) 





