#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup, Tag
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
art_counter = [link] * len(articles)

article_links = art_counter[:]
iteration = 1
while next_page:
    r = requests.get(f"http://gazetakulturalna.zelow.pl{next_page['href']}").content
    soup = BeautifulSoup(r, 'lxml')
    articles = soup.find_all('div', class_='art-article')
    art_counter = [f"http://gazetakulturalna.zelow.pl{next_page['href']}"] * len(articles)
    article_links.extend(art_counter)
    results.extend(articles)
    next_page = soup.find('a', string='Następny artykuł')
    print("{:.0%}".format(iteration/67))
    iteration += 1
    
all_results = [] 
for article, link in tqdm(zip(results, article_links), total=len(results)):
    
    if [e.find_all('span', attrs={'style': 'font-size: x-large; color: #000000;'})[0] for e in article.find_all('p') if e.find_all('span', attrs={'style': 'font-size: x-large; color: #000000;'})]:
    
        try:
            author = [e.find_all('span', attrs={'style': 'font-size: 12pt;'})[0] for e in article.find_all('p') if e.find_all('span', attrs={'style': 'font-size: 12pt;'})][0].text
        except IndexError:
            try:
                author = [e.find_all('span', attrs={'style': 'font-size: medium; color: #000000;'})[0] for e in article.find_all('p') if e.find_all('span', attrs={'style': 'font-size: medium; color: #000000;'})][0].text
            except IndexError:
                author = None
    
        title = [e.find_all('span', attrs={'style': 'font-size: x-large; color: #000000;'})[0] for e in article.find_all('p') if e.find_all('span', attrs={'style': 'font-size: x-large; color: #000000;'})][0].text
    
        text_of_article = ''.join([ele for ele in ['/n'.join([el.text for el in e.find_all('span', attrs={'style': 'font-family: arial, helvetica, sans-serif; font-size: 14pt;'})]) for e in article.find_all('p') if e.find_all('span', attrs={'style': 'font-family: arial, helvetica, sans-serif; font-size: 14pt;'})] if ele.strip()])
        
        zdjecia_grafiki = True if [x['src'] for x in article.find_all('img') if x.get('src')] else False
        
        try:
            opis_ksiazki = [e for i,e in enumerate(article) if i == article.index(article.find('p', string=re.compile('^\_+')))+2][0].text
        except ValueError:
            opis_ksiazki = None
           
        try: 
            photos_links = ' | '.join([f"http://gazetakulturalna.zelow.pl{x['src']}" for x in article.find_all('img')])  
        except (AttributeError, KeyError, IndexError):
            photos_links = None
        
        
        b = [e.find(('strong', 'em')) for e in article.find_all('p') if e.find(('strong', 'em'))]
        
        b_tags = [e.name for e in b]
        b_tags2 = [[el.name for el in e.descendants] for e in b]
        
        b_tags_all = list(zip(b_tags, [e[0] for e in b_tags2]))
        
        poem_titles = ' | '.join([e[0].text for e in (zip(b, b_tags_all)) if e[-1] == ('strong', 'em')])
    
        dictionary_of_article = {"Link": link, 
                                 "Data publikacji": None,
                                 "Tytuł artykułu": title,
                                 "Tekst artykułu": text_of_article,
                                 "Autor": author,
                                 'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img') if x.get('src')] else False,
                                 'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                                 'Linki do zdjęć': photos_links,
                                 'Tytuły wierszy': poem_titles,
                                 'Opis książki': opis_ksiazki
                                 }
        all_results.append(dictionary_of_article)


#%% writing files

with open(f'data/gazetakulturalnazelow_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)         
    
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/gazetakulturalnazelow_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/gazetakulturalnazelow_{datetime.today().date()}.xlsx", f'data/gazetakulturalnazelow_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()





















