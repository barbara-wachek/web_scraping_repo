import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor


from datetime import datetime
from time import mktime





def dyskretny_urok_drobiazgow_sitemap(link):
    sitemap = 'http://dyskretnyurokdrobiazgow.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
        
    
    def dictionary_of_article(link):
        html_text = requests.get(link).text
        while 'Error 503' in html_text:
            time.sleep(2)
            html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        dictionary_of_article = {}
        
        date_of_publication = soup.find('abbr')['title']
        date = re.sub(r'([\d-]*)(T.*)', r'\1', date_of_publication)
        
        try:
            
            texts_of_article = soup.find_all('div', class_='post-body')
            tags_span = soup.find_all('span', class_='label-info')
            tagi = [x.text.replace('\n', '').replace('  ', "").replace(',', ' | ') for x in tags_span]
      
        except AttributeError:
            pass 
        except IndexError:   
            pass
               
        
        for element in texts_of_article:
            try:
                article = element.text.strip().replace('\n', ' ')
               
                dictionary_of_article['Link'] = link
                dictionary_of_article["Autor"] = 'Kinga Piotrowiak-Junkiert'
                dictionary_of_article['Data publikacji'] = date
                
            
                title_of_article = soup.find('h3', class_='post-title').text.strip()
                dictionary_of_article['Tytuł artykułu'] = title_of_article
                
                
                dictionary_of_article['Tekst artykułu'] = article
                dictionary_of_article['Tagi'] = tagi[0]
                
                
                data_of_book_from_title = re.sub(r'([\d\-\s\w\'\"\„\”\.]*)(\(.*)', r'\2', title_of_article)     
                dictionary_of_article['Opis książki'] = data_of_book_from_title
             
            
            except AttributeError:
                pass 
            except IndexError:   
                pass
            
        
            try:
                list_of_images = [x['src'] for x in element.find_all('img')]
                if list_of_images != []:
                    dictionary_of_article['Zdjęcia/Grafika'] = 'TAK'
                    
                
                list_of_video = [x['src'] for x in element.find_all('iframe')]
                if list_of_video != []:
                    dictionary_of_article['Filmy'] = 'TAK'
                    
                
            except AttributeError:
                pass 
            except IndexError:   
                pass   
            
            

            all_results.append(dictionary_of_article)
    
    
    all_results = []
    
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_article, links),total=len(links)))
        
        
    df = pd.DataFrame(all_results)
    df.to_excel(f"dyskretny_urok_drobiazgow_{datetime.today().date()}.xlsx", index=False)   













