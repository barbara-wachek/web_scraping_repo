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
    # sitemap_link = 'https://teatralia.com.pl/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if ('artykuly' in x.text or x.text.count('/') == 4) and 'autorzy' not in x.text]
    return links


def dictionary_of_article(link):
    # link = 'https://teatralia.com.pl/10-lat-minelo/'
    # link = 'https://teatralia.com.pl/archiwum/artykuly/2011/styczen_2011/080111_dspo.php'
    # link = 'https://teatralia.com.pl/artykuly/2010/marzec_2010/160310_dngo.php'
    # link = 'https://teatralia.com.pl/ktoredy-domu/'
    # link = 'https://teatralia.com.pl/gala-na-miare-nienaszych-czasow-koncert-galowy-budorigum-czyli-amerykanscy-naukowcy-polskim-wroclawiu/'
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        try:
            date_of_publication = soup.find('time', {'class': 'published'})['datetime'][:10]
        except TypeError:
            try:
                date_of_publication = soup.find('p', {'class': 'prawo'}).find_all('strong')[-1].text
                date_of_publication = date_change_format_short(date_of_publication)
            except AttributeError:
                try:
                    date_of_publication = soup.find('div', {'class': 'byline'}).find_all('a')[1].text
                    date_of_publication = date_change_format_short(date_of_publication)
                except ValueError:
                    date_of_publication = soup.find('div', {'class': 'byline'}).find_all('abbr')[0].text
                    date_of_publication = date_change_format_short(date_of_publication)
                except AttributeError:
                    date_of_publication = soup.find('div', {'class': 'entry-meta'}).find_all('div')[2].text.strip()
                    date_of_publication = date_change_format_short(date_of_publication)
            except ValueError:
                date_of_publication = date_of_publication.split('\n')[-1]
                date_of_publication = date_change_format_short(date_of_publication)
       
        content_of_article = soup.find('div', class_='entry-content')
        if not content_of_article:
            content_of_article = soup.find('div', {'id': 'srodekszerszy_mniej'})
        
        tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')])
        
        try:
            author = [e.text for e in soup.find_all('a', rel='tag')][1]
            if author in ['recenzje', 'opera', 'relacje', 'festiwale']:
               author = [e.text for e in soup.find_all('a', rel='tag') if e.text not in ['recenzje', 'opera', 'relacje', 'festiwale']][0] 
            
        except IndexError:
            try:
                author = soup.find('p', {'class': 'prawo'}).find_all('strong')[0].text
            except AttributeError:
                author = ""
        if '#' in author:
            author = soup.find('a', {'class': 'author-link'}).text
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '').strip() for e in content_of_article]).strip()
        
        title_of_article = soup.find('h1', {'class': 'post-title entry-title'})
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            try:
                title_of_article = soup.find('p', {'class': 'tytul'}).text
            except AttributeError:
               try:
                   title_of_article = soup.find('h2', {'class': 'post-title entry-title'}).text
               except AttributeError:
                   title_of_article = soup.find('h1', {'class': 'entry-title'}).text
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('div')] for el in sub if 'teatralia' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link,
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tagi': tags,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    except (AttributeError, ValueError, TypeError):
        errors.append(link)
    except IndexError:
        pass
    
#%% main
articles_links = get_links_of_sitemap('https://teatralia.com.pl/sitemap.xml')

all_results = []
# do_over = errors.copy()

errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/teatralia_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/teatralia_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/teatralia_{datetime.today().date()}.xlsx", f'data/teatralia_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















