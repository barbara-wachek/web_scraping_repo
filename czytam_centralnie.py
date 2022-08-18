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
import json



#%% def

def czytam_centralnie_web_scraping_sitemap(sitemap):
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
  
    author = "Maciej Robert"
    title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
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
    tags = [tag.text for tag in tags_span][0].split()    
    
    for element in texts_of_article:
        try:
            article = element.text.strip().replace('\n', ' ')
           
            dictionary_of_article['Link'] = article_link
            dictionary_of_article['Autor'] = author
            dictionary_of_article['Tytuł artykułu'] = title_of_article
            dictionary_of_article['Data publikacji'] = new_date
           
            dictionary_of_article['Tekst artykułu'] = article
            dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
            
            dictionary_of_article['Autor książki'] = re.findall(r'(?<=\p{Lu}\s\-\s)[\p{L}\-\s\.]*(?=\"|\„.*\"|\”)', title_of_article)[0].strip() 
            dictionary_of_article['Tytuł książki'] = re.sub(r'([\p{Lu}\s0-9\.\,\-\'\!\(\)\"\„\”\=\?]*)(\s\-\s)([\p{L}\-\s\.\']*)([\"|\„].*[\"|\”]?)', r'\4', title_of_article)
            
        except AttributeError:
            pass 
        except IndexError:   
            pass
        all_results.append(dictionary_of_article)
    
#%% main

sitemap_links = czytam_centralnie_web_scraping_sitemap('http://czytamcentralnie.blogspot.com/sitemap.xml')    
articles_links = []
 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))

all_results = []
    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

with open(f'czytam_centralnie_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)       

    
df = pd.DataFrame(all_results)
df = df.drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)
df.to_excel(f"czytam_centralnie_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
    
       
        
       
        
       
        
       
        
       
        
       
        
       
        
         
       
