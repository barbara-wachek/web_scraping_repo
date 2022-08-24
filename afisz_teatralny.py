#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json


#%%def
def afisz_teatralny_web_scraping(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links  

def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.findall(r'pt\-page-', link)]
    articles_links.extend(sitemap_links) 


def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_article = {}
    date_of_publication = re.sub(r'(http:\/\/www\.afiszteatralny\.pl\/)(\d{4})(\/)(\d{2})(\/[\w\.\-]*)', r'\2-\4', article_link)
    
    try:
        texts_of_article = soup.find_all('div', class_='post-entry')
        title_of_article = soup.find('div', class_='post-header').h1.text.strip()
        tags = " ".join([x.text.replace("\n", " ").strip() for x in soup.find_all('div', class_='entry-tags gray-2-secondary')])
        
    except AttributeError:
        pass 
    except IndexError:   
        pass
    
    for element in texts_of_article:
        try:
            article = element.text.strip()
           
            dictionary_of_article['Link'] = article_link
            dictionary_of_article['Data publikacji'] = date_of_publication  #brak dziennej daty dlatego nie mogę później zamienić na format datetime
            dictionary_of_article['Autor'] = 'Agnieszka Kobroń' #sprawdzic, czy kazdy post jej jej autorstwa
            dictionary_of_article['Tytuł artykułu'] = title_of_article
            dictionary_of_article['Tekst artykułu'] = article
            
            tagi = re.sub(r'(Tags\:\s)(.*)', r'\2', tags).replace(",", " |")
            dictionary_of_article['Tagi'] = tagi.replace("Tags:", "")  #Można by ewentualnie popracować na wyjęciem z tagów nazw teatrów
            
            if re.search(r'reż.', title_of_article):
                dictionary_of_article['Tytuł spektaklu'] = re.sub(r'(.*)(\,\sreż\.)(.*)', r'\1', title_of_article)
                dictionary_of_article['Reżyser'] = re.sub(r'(.*)(\sreż\.)(.*)', r'\3', title_of_article).strip()
             
        except AttributeError:
            pass 
        except IndexError:   
            pass
        
        try:
            links_in_article = [x['href'] for x in element.find_all('a')]
            dictionary_of_article['Linki zewnętrzne'] = ' | '.join([x for x in links_in_article if not re.findall(r'blogspot|jpg', x)])
            
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
        except KeyError:
            pass
        
        
        try:
            links_of_images = [x['src'] for x in element.find_all('img')]
            dictionary_of_article['Linki do zdjęć'] = ' | '.join(links_of_images)
                    
        except AttributeError:
            pass 
        except IndexError:   
            pass        

        all_results.append(dictionary_of_article)    



#%%main

sitemap_links = afisz_teatralny_web_scraping('http://www.afiszteatralny.pl/sitemap.xml')

articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))
    
all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   
    
with open(f'afisz_teatralny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)     
    
    
df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"afisz_teatralny_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')    
    writer.save()       
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    