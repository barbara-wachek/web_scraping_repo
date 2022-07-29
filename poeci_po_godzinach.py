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

import selenium




def poeci_po_godzinach_web_scraping_sitemap(link):
    sitemap = 'https://poecipogodzinach.blogspot.com/sitemap.xml'
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
        
        #article_link = 'http://poecipogodzinach.blogspot.com/2020/12/potrafie-czuwac.html'
        #article_link = 'http://poecipogodzinach.blogspot.com/2016/05/ktos-chcia-widziec-we-mnie-ciagle-nowe.html'
        
        #article_link = 'http://poecipogodzinach.blogspot.com/2018/06/smutna-jest-moja-dusza-az-do-smierci.html'
        article_link = 'http://poecipogodzinach.blogspot.com/2018/12/wszystko-juz-byo-w-srodku-na-zewnatrz.html'
        
        
        html_text = requests.get(article_link).text
        while 'Error 503' in html_text:
            time.sleep(2)
            html_text = requests.get(article_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        dictionary_of_article = {}
        
        
        
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
        tags = [tag.text for tag in tags_span][0].strip().split('\n')
        
        
        
        for element in texts_of_article:
            try:
                article = element.text.strip().replace('\n', ' ')
               
                dictionary_of_article['Link'] = article_link
                dictionary_of_article['Tytuł artykułu'] = title_of_article
                dictionary_of_article['Data publikacji'] = new_date
                
            
                dictionary_of_article['Tekst artykułu'] = article
                
                dictionary_of_article['Autor wiersza'] = re.findall(r'^\p{Lu}{1}[\p{Ll}.]*\s\p{Lu}{1}\p{Ll}*\-?\p{Lu}*\p{Ll}*', article)[0]
                titles_of_poems = [x.text for x in element.find_all('b')]
                
                #titles_of_poems = [x.text for x in element.find_all('b', attrs={'font-size': 'x-large'})]
                #do poprawienia na podstawie filmiku
                
                dictionary_of_article['Tytuły wierszy'] = titles_of_poems
                
                
                dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
                
                
                list_of_images = [x for x in element.find_all('img')]
                
                #Następnym krokiem będzie stworzenie listy linków i pobranie zdjęć z tych linków. Pytanie gdzie zapisywać takie obrazy i jak je nazywać
                #żeby móc je łatwo potem dopasować do artykułu 
                link_image = re.sub(r'(src\=\")(https:\/\/.*)(\"\s)', r'\1', list_of_images)
                #nazwa pliku ze zdjęciem powinna zawierać w nazwie tytuł bloga i dzień publikacji? i stworzyć lokalny folder do zapisywania zdjęć
                #
                
                
                for image in list_of_images:
                    link_image = re.sub(r'(src\=\")(https:\/\/.*)(\"\s)', r'\1', image)
                    dictionary_of_article['Zdjęcia'] = 'TAK'
                    
               
                
                
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
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        