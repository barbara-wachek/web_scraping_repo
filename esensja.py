#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from time import mktime
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from functions import date_change_format_short


#%% def

def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'https://esensja.pl/magazyn/'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    dates = [e.text for e in soup.find_all('td', {'colspan': '2', 'valign': 'top', 'align': 'center'}) if e.text.strip()]
    links = ['https://esensja.pl/magazyn/' + e.find('a')['href'] for e in soup.find_all('td', {'valign': 'top', 'width': '105'}) if 'index' in e.find('a')['href']]
    links = [e.replace('index', 'iso/01') for e in links]
    art_links_total = []
    for date, link in tqdm(zip(dates, links), total=len(links)):
        # link = links[0]
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, "html.parser")
        art_links = [e['href'] for e in soup.select('.n-title a') if 'http' in e['href']]
        for art_link in art_links:
            art_links_total.append((art_link, date))
    return art_links_total

def dictionary_of_article(link):
    # link = articles_links[20]
    # link = 'https://moznaprzeczytac.pl/fantasyexpo-wroclaw-13-10-2013/'
    # link = ['http://esensja.pl/tworczosc/opowiadania/tekst.html?id=35230']
    try:
        html_text = requests.get(link[0]).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        try:
            date_of_publication = soup.find('meta', {'itemprop': 'datePublished'})['content']
        except TypeError:
            if soup.find('div', {'id': 'strony'}):
                
                pattern = re.compile(r'strona=(\d+)')
                links = [(int(pattern.search(a["href"]).group(1)), a["href"]) for span in soup.find_all('span', {'class': 'tn-link'}) if (a := span.find("a")) and pattern.search(a["href"])]
                last_page = '/'.join(link[0].split('/')[:-1]) + '/' + max(links, key=lambda x: x[0])[1] if links else None
                html_text2 = requests.get(last_page).text
                soup2 = BeautifulSoup(html_text2, 'html.parser')
                date_of_publication = date_change_format_short(soup2.find('div', {'class': 'data'}).text)                
            else:
                date_of_publication = date_change_format_short(soup.find('div', {'class': 'data'}).text)
               
        content_of_article = soup.find('div', class_='tresc')
        
        tags = '|'.join([e['content'] for e in soup.find_all('meta', {'itemprop': 'keywords'})])
        
        try:
            author = soup.find('div', {'itemprop': 'author'}).text
        except AttributeError:
            author = soup.find('div', {'class': 'autor deskOnly'}).text
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
        
        title_of_article = soup.find('meta', {'itemprop': 'name'})
     
        if title_of_article:
            title_of_article = title_of_article['content']   
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('div')] for el in sub if 'href' in el.attrs and 'esensja' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link[0],
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except:
        errors.append(link)
    
#%% main
articles_links = get_links_of_sitemap('https://esensja.pl/magazyn/')
articles_links = [e for e in articles_links if '_redakcja' not in e[0]]

all_results = []
# do_over = errors.copy()

errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/esensja_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/esensja_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/esensja_{datetime.today().date()}.xlsx", f'data/esensja_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















