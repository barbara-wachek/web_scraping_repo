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
from time import mktime
import json


#%%def
def jerzy_sosnowski_web_scraping(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc') if not re.findall(r'sitemap\-misc\.xml',e.text) if not re.findall(r'pt\-page-',e.text)]
    extras = [x.text for x in soup.find_all('loc') if re.findall(r'pt\-page-', x.text)]
    return links, extras  

def get_article_pages(link):   
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if not re.findall(r'pt\-page-', link)]
    articles_links.extend(sitemap_links) 
    
def get_extras_pages(link): 
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    extras = [e.text for e in soup.find_all('loc') if re.findall(r'pt\-page-', link)]
    extras_links.extend(extras)
    

def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_article = {}
    
    #DATA
    
    date_of_publication = soup.find('span', class_="post-time").text
    date = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s[\w]*\s[\w]*)', r'\2\3\4', date_of_publication).strip()
   
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)
    
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    
    try:
        author = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s)([\w]*\s[\w]*)', r'\6', date_of_publication).strip()
        texts_of_article = soup.find_all('div', class_='e-content entry-content')
        title_of_article = soup.find('h1', class_='p-name entry-title').text
        
  
    except AttributeError:
        pass 
    except IndexError:   
        pass
        
    
    for element in texts_of_article:
        try:
            article = element.text.strip().replace('\n', ' ')
           
            dictionary_of_article['Link'] = article_link
            dictionary_of_article['Data publikacji'] = new_date
            dictionary_of_article['Autor'] = author
            dictionary_of_article['Tytuł artykułu'] = title_of_article
            dictionary_of_article['Tekst artykułu'] = article
        
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
        
        
def extras_content(extras_link):
    html_text = requests.get(extras_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_extras = {}
    
    try:
        dictionary_of_extras['Link'] = extras_link
        dictionary_of_extras['Tytuł'] = soup.find('h2').text
        text_of_extras = soup.find('div', attrs={'id':'page'})
        dictionary_of_extras['Tekst'] = " ".join([x.text.replace('\xa0', '') for x in text_of_extras.select('p')])
        links_of_images = [x['src'] for x in soup.find_all('img')]
        dictionary_of_extras['Linki do zdjęć'] = ' | '.join(links_of_images)
        
    except AttributeError:
         pass 
    except IndexError:   
        pass       

    extras_results.append(dictionary_of_extras)   

#%%main


sitemap_links = jerzy_sosnowski_web_scraping('https://jerzysosnowski.pl/sitemap.xml')[0]
extras_links_sitemap = jerzy_sosnowski_web_scraping('https://jerzysosnowski.pl/sitemap.xml')[1]

articles_links = []

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))

extras_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_extras_pages, extras_links_sitemap),total=len(extras_links_sitemap)))

   
all_results = [] 

with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

extras_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(extras_content, extras_links),total=len(extras_links)))
    
    
with open(f'jerzy_sosnowski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 
with open(f'jerzy_sosnowski_extras_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(extras_results, f)             
    
    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)

df_extras = pd.DataFrame(extras_results).drop_duplicates()


with pd.ExcelWriter(f"jerzy_sosnowski_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    df_extras.to_excel(writer, 'Pages', index=False, encoding='utf-8')   
    writer.save()    




















