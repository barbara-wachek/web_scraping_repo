import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import regex as re

#Do wysyłania raportów na maila przez pocztę ibl.waw.pl
import smtplib
import ssl
from email.mime.text import MIMEText

import time
import sys
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor




#Od 05.2012 trzeba korzystać z sitemapy - muszę napisać nowy kod
# Web scraping Krytycznym okiem wedlug sitemapy strony
#site_map_link = 'http://www.krytycznymokiem.blogspot.com/sitemap.xml'
#link = 'http://krytycznymokiem.blogspot.com/sitemap.xml?page=9'




#ZADANIE OD CR: Zrób pętle, która działa na article_links zbiera info i oddaje tabelę

def krytycznym_okiem_web_scraping_sitemap():
    
    sitemap = 'http://krytycznymokiem.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    
    articles_links = []    
    all_results = []
 
    for link in tqdm(links):
        #link = links[0]
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        sitemap_links = [e.text for e in soup.find_all('loc')]
        #len( articles_links)
        articles_links.extend(sitemap_links)

  
    
    for e in tqdm(articles_links):
        html_text = requests.get(e).text
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
                
                dictionary_of_article['Link'] = e
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
    
    
    
    df = pd.DataFrame(all_results)
    df.to_excel(f"krytycznym_okiem_all_articles_test.xlsx", encoding='utf-8', index=False) 




#ZADANIE OD CR (DRUGI WARIANT): #Zrob petle z uzyciem funkcji map(), która zbiera info i oddaje tabele (wymaga stworzenia funkcji)




def krytycznym_okiem_web_scraping_sitemap():
    
    sitemap = 'http://krytycznymokiem.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    
    articles_links = []    
    all_results = []
    
    
    def get_article_pages(link):
        
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        sitemap_links = [e.text for e in soup.find_all('loc')]
        #len( articles_links)
        #articles_links.extend(sitemap_links)
            
    articles_links = list(map(get_article_pages, links))
        
    def dictionary_of_data_from_article(link):
       html_text = requests.get(link).text
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
                
                dictionary_of_article['Link'] = e
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


    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_data_from_article, articles_links),total=len(links)))        
            
       
        
                 
        
    
    
    
    df = pd.DataFrame(all_results)
    df.to_excel(f"krytycznym_okiem_all_articles.xlsx", encoding='utf-8', index=False) 







# ZADANIE OD CR (TRZECI WARIANT): Petla uzyciem funkcji map() i wielowątkowosci, która zbiera info i oddaje tabele (wymaga stworzenia funkcji)


def krytycznym_okiem_web_scraping_sitemap():
    
    sitemap = 'http://krytycznymokiem.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    
    articles_links = []    
    
 
    def get_article_pages(link):   
    
        #link = links[0]
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        sitemap_links = [e.text for e in soup.find_all('loc')]
        #len( articles_links)
        articles_links.extend(sitemap_links)
        
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(get_article_pages, links),total=len(links)))
  
    
  
    def dictionary_of_article(article_link):
        
        #article_link = articles_links[850]
        #article_link = 'http://krytycznymokiem.blogspot.com/2018/05/szpadel-lize-spit.html'
    #for article_link in articles_links[840:860]:
        #html_text potem soup i potem while, kryterium: parsowanie soup i zobaczyć, czy w h2 error 503; jeli bedzie tam
        #blad czekac w while i jeszcze raz robic html_text i jeszcze raz soup
        
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
    df.to_excel(f"krytycznym_okiem_all_articles_test_wielowatkowosc.xlsx", encoding='utf-8', index=False) 









   def get_article_pages(link):
        for link in links:
            link = links[0]
            html_text_sitemap = requests.get(link).text
            soup = BeautifulSoup(html_text_sitemap, 'lxml')
            sitemap_links = [e.text for e in soup.find_all('loc')]
            #len( articles_links)
            articles_links.extend(sitemap_links)
        
        
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(get_article_pages, links),total=len(links)))





# get_article_pages('http://krytycznymokiem.blogspot.com/sitemap.xml?page=1')
# test = list(map(get_article_pages, links))

# numbers = [2, 4, 6, 8, 10]

# # returns square of a number
# def square(number):
#   return number * number

# # apply square() function to each item of the numbers list
# squared_numbers_iterator = map(square, numbers)

# # converting to list
# squared_numbers = list(squared_numbers_iterator)
# print(squared_numbers)
  

    




    
    link = 'http://krytycznymokiem.blogspot.com/sitemap.xml?page=1'
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    #len(links)
    all_results = []

    for entry_link in links:
        #entry_link = 'http://krytycznymokiem.blogspot.com/2021/03/mireczek-patoopowiesc-o-moim-ojcu.html'
        
        html_text_link = requests.get(entry_link).text
        soup_link = BeautifulSoup(html_text_link, 'lxml')
        
        dictionary_of_article = {}
   
        author = "Jarosław Czechowicz"
        title_of_article = soup_link.find('h3', class_='post-title entry-title').text.strip()
        author_of_book = re.findall(r'(?<=[\"|\”]\s)(.*\s*\.*\s*?\.*?)', title_of_article)  
        title_of_book = re.findall(r'[\"\„].*[\"\”](?=\s.*)', title_of_article)
        date_of_publication = soup_link.find('h2', class_='date-header').text
        texts_of_article = soup_link.find_all('div', class_='post-body entry-content')
        tags = soup_link.find('span', class_='post-labels')         
        
        #month = re.findall(r'(?<=com\/)(\d{4})(\/)(\d{2})(?=.*)', entry_link)[2]
        #year = re.findall(r'(?<=com\/)(\d{4})(\/)(\d{2})(?=.*)', entry_link)[0]
        
                     
        for element in texts_of_article:
            try:
                article = element.text.strip()
                
                dictionary_of_article['Link'] = entry_link
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
    
    
    
    df = pd.DataFrame(all_results)
    df.to_excel(f"krytycznym_okiem_03_2021_06_2022.xlsx", encoding='utf-8', index=False) 