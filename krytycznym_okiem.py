import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import regex as re

import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor



def krytycznym_okiem_web_scraping_sitemap():
    
    sitemap = 'http://krytycznymokiem.blogspot.com/sitemap.xml'
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
        tags = soup.find('span', class_='post-labels')     
        
        
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
                
                dictionary_of_article['Odautorski tytuł recenzji'] = re.findall(r'(?<=Tytuł recenzji:\s).*\s?.*?(?=\p{Lu}|\n)', article)[0].replace('\n', ' ').strip() #Autor bloga zmienił schemat danych w maju 2013 (tytuł recenzji podaje w leadzie)
                dictionary_of_article['Data wydania książki'] = re.findall(r'(?<=Data wydania:\s).*(?=\n)', article)[0]
                dictionary_of_article['Wydawnictwo'] = re.findall(r'(?<=Wydawca:\s).*\s?.*?(?=\n|D)', article)[0].replace('\n', ' ').strip()
                
                dictionary_of_article['Tagi'] = tags.a.text
                
                
            except AttributeError:
                pass 
            except IndexError:   
                pass
            all_results.append(dictionary_of_article)
    
    all_results = []
    
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    
    
    df = pd.DataFrame(all_results)
    df.to_excel(f"krytycznym_okiem_all.xlsx", encoding='utf-8', index=False) 