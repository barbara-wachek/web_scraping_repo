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
from time import mktime

#%% def

def bernadetta_darska_web_scraping_sitemap(sitemap):
    # sitemap = 'https://bernadettadarska.blogspot.com/sitemap.xml'
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
   
    author = "Bernadetta Darska"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
    title_of_book = re.sub(r'(.*\()([\w\.]*\s.*)(\,\s)(\p{Lu}.*)(\)$)', r'\4', title_of_article)     
    date_of_publication = soup.find('h2', class_='date-header').text
    date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication)
    
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    s = date
    for k, v in lookup_table.items():
        s = s.replace(k, v)
    
    result = time.strptime(s, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())      
  
    
    texts_of_article = soup.find_all('div', class_='post-body entry-content')
    tags_span = soup.find_all('span', class_='post-labels')
    tags = [tag.text for tag in tags_span][0].strip().split('\n')        
    

    for element in texts_of_article:
        try:
            article = element.text.strip().replace('\n', ' ')
           
            dictionary_of_article['Link'] = article_link
            dictionary_of_article['Autor'] = author
            dictionary_of_article['Tytuł artykułu'] = title_of_article
            dictionary_of_article['Data publikacji'] = new_date
           
            dictionary_of_article['Tekst artykułu'] = article.replace('\n', ' ')
            
            book_description = re.findall(r'(?<=\s{3}|\s{4}|\s{5}|\s{6}).*', article)[0]
            
                
            publisher = re.sub(r'(.*)(\,\s)([\w\s\-\.]*)(\,\s)(.*)(\.$)', r'\3', book_description)
            place_and_year = re.sub(r'(.*)(\,\s)([\w\s\-\.]*)(\,\s)(.*)(\.$)', r'\5', book_description)
            
            
            dictionary_of_article['Opis książki'] = book_description.strip()
            dictionary_of_article['Autor książki'] = re.findall(r'^[\p{L}\s\-\.]+(?=\,)', book_description)[0].strip()
            dictionary_of_article['Tytuł książki'] = title_of_book
            dictionary_of_article['Wydawnictwo'] = publisher
            dictionary_of_article['Rok i miejsce wydania'] = place_and_year
            
            dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
            
            
        except AttributeError:
            pass 
        except IndexError:   
            pass
        all_results.append(dictionary_of_article)




#%% main

sitemap_links = bernadetta_darska_web_scraping_sitemap('https://bernadettadarska.blogspot.com/sitemap.xml')

articles_links = [] 
   
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links))) 
   
all_results = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    

df = pd.DataFrame(all_results)
df.to_excel(f"bernadetta_darska_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   