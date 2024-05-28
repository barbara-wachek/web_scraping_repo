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
import json
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#%%def

def get_article_links(issues_url):
    response = requests.get(issues_url)
    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    li_tags = soup.find_all('li', {'dir': 'ltr'})
    a_tags = [li.find_all('a') for li in li_tags]
    a_tags = [a for sub in a_tags for a in sub]
    a_tags = [a for a in a_tags if a.has_attr('href') and '/numery-czasopisma/' in a['href']]
    issues_links = [('https://www.polisemia.com.pl' + a['href'], a.text) for a in a_tags]
    output = []
    for link, name in tqdm(issues_links):
        resp = requests.get(link)
        html_text = resp.text
        soup = BeautifulSoup(html_text, 'lxml')
        sections = soup.find_all('section')
        articles_links = [section.find_all('a') for section in sections]
        articles_links = [a for sub in articles_links for a in sub]
        articles_links = [(name, a.text, 'https://www.polisemia.com.pl' + a['href']) for a in articles_links if a and a.has_attr('href') and '/numery-czasopisma/' in a['href'] and '/konferencja' not in a['href']]
        output.extend(articles_links)
    output_dict = {}
    for elem in output:
        output_dict.setdefault(elem[2], list()).append((elem[0], elem[1]))
    output_list = []
    for key, value in output_dict.items():
        issue = None
        article = []
        for elem in value:
            issue = elem[0]
            article.append(elem[1])
        article = ' '.join(article)
        output_list.append((key, issue, article))
    return output_list
    
    
def dictionary_of_article(article_tuple):
    article_link, issue, author_title = article_tuple
    
    response = requests.get(article_link)
    if not response.ok:
        return

    html_text = response.text
    soup = BeautifulSoup(html_text, 'lxml')
    
    # date
    try:
        date_of_publication = issue.replace('–', '-').split(' - ')[0]
        date_of_publication = datetime.strptime(date_of_publication, '%m/%Y')
        date_of_publication = date_of_publication.strftime('%Y-%m-%d')
    except ValueError:
        date_of_publication = None
    
    # content
    if art_content := soup.find('div', {'role': 'main'}):
        text_of_article = art_content
        article = text_of_article.text.strip().replace('\n', ' ')
    else:
        text_of_article, article = None, None
         
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a') if x['href']]])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img') if x['src']])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
            

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor i Tytuł': author_title,
                             'Numer': issue,
                             'Tekst artykułu': article,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links,
                             }
    all_results.append(dictionary_of_article)

#%%
issues_url = 'https://www.polisemia.com.pl/numery-czasopisma'
articles_links = get_article_links(issues_url)

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
    


with open(f'data/polisemia_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"data/polisemia_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()   

#%%Uploading files on Google Drive

gauth = GoogleAuth()      
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/polisemia_{datetime.today().date()}.xlsx", f'data/polisemia_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  