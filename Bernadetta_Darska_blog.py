#%% import 
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
import functions as fun


#%% def    
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
    
    author = "Bernadetta Darska"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    title_of_book = re.sub(r'(.*\()([\w\.]*\s.*)(\,\s)(\p{Lu}.*)(\)$)', r'\4', title_of_article)     
    date_of_publication = soup.find('h2', class_='date-header').text
    new_date = fun.date_change_format_long(date_of_publication)
    texts_of_article = soup.find('div', class_='post-body entry-content')
    article = texts_of_article.text.strip().replace('\n', ' ')
    tags_span = soup.find_all('span', class_='post-labels')
    tags = [tag.text for tag in tags_span][0].strip().split('\n')    
    
    dictionary_of_article = {}

    try:
        
        dictionary_of_article['Link'] = article_link
        dictionary_of_article['Autor'] = author
        dictionary_of_article['Tytuł artykułu'] = title_of_article
        dictionary_of_article['Data publikacji'] = new_date  
        dictionary_of_article['Tekst artykułu'] = article.replace('\n', ' ')
        
        book_description = re.findall(r'(?<=\s{3}|\s{4}|\s{5}|\s{6}).*', article)[0]    
        dictionary_of_article['Opis książki'] = book_description.strip()
        
        dictionary_of_article['Autor książki'] = re.findall(r'^[\p{L}\s\-\.]+(?=\,)', book_description)[0].strip()
        dictionary_of_article['Tytuł książki'] = title_of_book
        
        publisher = re.sub(r'(.*)(\,\s)([\w\s\-\.]*)(\,\s)(.*)(\.$)', r'\3', book_description)
        dictionary_of_article['Wydawnictwo'] = publisher
        
        place_and_year = re.sub(r'(.*)(\,\s)([\w\s\-\.]*)(\,\s)(.*)(\.$)', r'\5', book_description)    
        dictionary_of_article['Rok i miejsce wydania'] = place_and_year
        
        dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
        
    except AttributeError:
        pass 
    except IndexError:   
        pass
    all_results.append(dictionary_of_article)


#%% main
sitemap_links = fun.get_links('https://bernadettadarska.blogspot.com/sitemap.xml')
   
all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'bernadetta_darska_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)    

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"bernadetta_darska__{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
   
   
   
   
   
   
   
   
   
   
   
   