import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor

import locale

from datetime import datetime
from time import mktime





def czytam_centralnie_web_scraping_sitemap(link):
    
    sitemap = 'http://czytamcentralnie.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    
    articles_links = []    
    
    def get_article_pages(link):   
       
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        sitemap_links = [e.text for e in soup.find_all('loc')]
        articles_links.extend(sitemap_links)
       
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(get_article_pages, links),total=len(links)))
        
    def dictionary_of_article(article_link):
        #article_link = 'http://czytamcentralnie.blogspot.com/2017/03/biograficzna-powiesc-muzyczna-josef_2.html'
        article_link = 'http://czytamcentralnie.blogspot.com/2015/01/melancholia-tropikow-joanna-bator-wyspa.html'
        #article_link = 'http://czytamcentralnie.blogspot.com/2012/03/pozegnanie-z-agaja-agaja-veteranyi-kto.html'
        #article_link = 'http://czytamcentralnie.blogspot.com/2012/03/uwaga-dzieo-wybitne-josef-skvorecky.html'

        
        
        html_text = requests.get(article_link).text
        while 'Error 503' in html_text:
            time.sleep(2)
            html_text = requests.get(article_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        dictionary_of_article = {}
      
        author = "Maciej Robert"
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
           
        #Zamiana formatu daty - do poprawy wyskakuje błąd ValueError: time data '18 październik 2020' does not match format '%d %B %Y'
        
        date_of_publication = soup.find('h2', class_='date-header').text
        date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication)
   
        lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
        s = date
        for k, v in lookup_table.items():
            s = s.replace(k, v)
        
        #z = '18 października 2020'
        result = time.strptime(s, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())    
      
        title_of_book = re.sub(r'(^.*)(\s\-\s)([\p{L}\-\s\.]*)(\"|\„.*\"|\”)', r'\4', title_of_article)  
        texts_of_article = soup.find_all('div', class_='post-body entry-content')
        #tags = soup.find_all('a', href=True)
        #tags_span = soup.find_all('span', class_='post-labels')
        
        
        for element in texts_of_article:
            try:
                article = element.text.strip().replace('\n', ' ')
               
                dictionary_of_article['Link'] = article_link
                dictionary_of_article['Autor'] = author
                dictionary_of_article['Tytuł artykułu'] = title_of_article
                dictionary_of_article['Data publikacji'] = date
               
                dictionary_of_article['Tekst artykułu'] = article
                
                dictionary_of_article['Autor książki'] = re.findall(r'(?<=\p{Lu}\s\-\s)[\p{L}\-\s\.]*(?=\"|\„.*\"|\”)', title_of_article)[0].strip() 
                dictionary_of_article['Tytuł książki'] = title_of_book
                #dictionary_of_article['Tagi'] = '|'.join([e.text for e in tags if 'search/label' in e['href']])
                #['Tagi'] = '|'.join([x.a.text for x in tags_span])
                
                    
                
            except AttributeError:
                pass 
            except IndexError:   
                pass
            all_results.append(dictionary_of_article)
    
    
    all_results = []
    
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
        
    df = pd.DataFrame(all_results)
    df.to_excel(f"czytam_centralnie_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
        
       
       
       
 #PROBLEMY:


#do naprawy kod generujący datę
#do wyciągnięcia opis książki (dół artykułu)      
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
        
        