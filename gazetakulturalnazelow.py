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
from functions import date_change_format_short, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def

def create_dictionary_of_article(article_link):
    
    try:
        html_text = requests.get(article_link).text
        while '429 Too Many Requests' in html_text:
            time.sleep(5)
            html_text = requests.get(article_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        date_of_publication = re.findall('\d{4}\/\d{2}\/\d{2}', article_link)[0].replace('/', '-')
        article = soup.find('article', itemprop="blogPost")
        text_of_article = soup.find('div', class_='post-entry-text')
        text_of_article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
        author = "Marta Fox"
        title_of_article = soup.find('h1', itemprop='headline').text
        categories = soup.find_all('a', rel='category tag')
        categories = ' | '.join([e.text for e in categories])
        tags = soup.find_all('a', rel='tag')
        tags = ' | '.join([e.text for e in tags if e.text not in categories]  )
        
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'martafox\.pl', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None   
        try: 
            photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None
        
        dictionary_of_article = {"Link": article_link, 
                                 "Data publikacji": date_of_publication,
                                 "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                                 "Tekst artykułu": text_of_article,
                                 "Autor": author,
                                 "Kategoria": categories,
                                 "Tagi": tags,
                                 'Linki zewnętrzne': external_links,
                                 'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img') if x.get('src')] else False,
                                 'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                                 'Linki do zdjęć': photos_links
                                 }
        
        all_results.append(dictionary_of_article)    
    except AttributeError:
        return create_dictionary_of_article(article_link)

#%%main 

link = 'http://gazetakulturalna.zelow.pl/index.php'

results = []

r = requests.get(link).content
soup = BeautifulSoup(r, 'lxml')
articles = soup.find_all('div', class_='art-article')
results.extend(articles)
next_page = soup.find('a', string='Następny artykuł')


iteration = 1
while next_page:
    r = requests.get(f"http://gazetakulturalna.zelow.pl{next_page['href']}").content
    soup = BeautifulSoup(r, 'lxml')
    articles = soup.find_all('div', class_='art-article')
    results.extend(articles)
    next_page = soup.find('a', string='Następny artykuł')
    print("{:.0%}".format(iteration/67))
    iteration += 1
    

results[1]

results[1].find('p').find_all('span', attrs={'style': 'font-size: 12pt'})    


[e.text for e in next_page]



html_text_sitemap = requests.get(link).text
soup = BeautifulSoup(html_text_sitemap, 'lxml')
links_pages = [e.text for e in soup.find_all('loc')]
return links_pages






articles_links = [e for e in get_links('https://martafox.pl/sitemap-1.xml') if 'martafox.pl/2' in e]

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(create_dictionary_of_article, articles_links),total=len(articles_links)))

#%% writing files

with open(f'data/martafox_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)         
    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/martafox_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/martafox_{datetime.today().date()}.xlsx", f'data/martafox_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()





















