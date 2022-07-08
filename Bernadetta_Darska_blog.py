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



def bernadetta_darska_web_scraping_sitemap(link):
    
    sitemap = 'https://bernadettadarska.blogspot.com/sitemap.xml'
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
        #article_link = 'https://bernadettadarska.blogspot.com/2020/10/sowa-s-gromadzki-zydowka.html'
        #article_link = 'http://czytamcentralnie.blogspot.com/2015/01/melancholia-tropikow-joanna-bator-wyspa.html' #- z innego bloga
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
        #[x.a.text for x in tags_span]
        
        #tags = soup.find_all('a', href=True)  #Kod CR
        
        #tags = [x for x in soup.find_all('a', href=True) if 'Etykiety' in x.text]
        tags = soup.find_all('span', class_='post-labels')
        #dictionary_of_article['Tagi'] = []
        #for element in tags:
            #dictionary_of_article['Tagi'].append(element.a.text)
    
        #for tag in tags: 
            #dictionary_of_article['Tagi'] = tag.text
    
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
                
                #dictionary_of_article['Tagi'] = '|'.join([e.text for e in tags if 'search/label' in e['href']])
                #dictionary_of_article['Tagi'] = '|'.join([x.a.text for x in tags_span])
                
                dictionary_of_article['Tagi'] = []
                for element in tags:
                    dictionary_of_article['Tagi'].append(element.a.text)
                    
                '|'.join(dictionary_of_article['Tagi'])    
                
            except AttributeError:
                pass 
            except IndexError:   
                pass
            all_results.append(dictionary_of_article)
    
    
    all_results = []
    
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
        

    df = pd.DataFrame(all_results)
    df.to_excel(f"bernadetta_darska_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)

   
   
   #Problemy do rozwiązania: 
       #Wieloautorstwo. Czy to bedzie do ręcznej weryfikacji? Wydaje mi się, że nie można utworzyć regexa, który obejmie te przypadki
       
       #Czasami zdarzają się artykuły, które nie są rec., np. https://bernadettadarska.blogspot.com/2015/12/wesoych-swiat.html, ale raczej będzie łatwo je wyłapać ręcznie
           #bo będą miały dużo pustych pól w kolumnach
       #Co z nadawaniem typu artykułowi? Czy zajmować się tym na tym etapie? Czy to potem ktos będzie ustalał czy dany wpis jest recenzją czy cyzm innym
    
        
   #Przykłady zapisów błędnych: https://bernadettadarska.blogspot.com/2016/02/sowem-i-memem-o-kulturze-graff-m-frej.html (w pliku wiersz 18)
     
        
   