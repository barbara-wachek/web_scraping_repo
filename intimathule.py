#%%import
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



#%%def
def poeci_po_godzinach_web_scraping_sitemap(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(link):
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_article = {}
    
    date_of_publication = soup.find('h2', class_='date-header').text
    date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication)
   
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    s = date
    for k, v in lookup_table.items():
        s = s.replace(k, v)
    
    result = time.strptime(s, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())    
    
    try:
        texts_of_article = soup.find_all('div', class_='post-body')
    except AttributeError:
        pass 
    except IndexError:   
        pass
           
    
    for element in texts_of_article:
        try:
            article = element.text.strip().replace('\n', ' ')
           
            dictionary_of_article['Link'] = link
            dictionary_of_article["Autor"] = 'Joanna Mueller (podp. Epifania z Maligno)'
            dictionary_of_article['Data publikacji'] = new_date
            dictionary_of_article['Tytuł artykułu'] = soup.find('h3', class_='post-title').text.strip()
            dictionary_of_article['Tekst artykułu'] = article
         
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
        
        
        try:
            links_of_images = [x['src'] for x in element.find_all('img')]
            dictionary_of_article['Linki do zdjęć'] = ' | '.join(links_of_images)
                    
        except AttributeError:
            pass 
        except IndexError:   
            pass       

        all_results.append(dictionary_of_article)

    
    
#%% main
    
sitemap_links = poeci_po_godzinach_web_scraping_sitemap('https://intimathule.blogspot.com/sitemap.xml')
all_results = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, sitemap_links),total=len(sitemap_links)))

with open(f'intimathule_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        

df = pd.DataFrame(all_results)
df = df.drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)
df.to_excel(f"intimathule_{datetime.today().date()}.xlsx", index=False)   

