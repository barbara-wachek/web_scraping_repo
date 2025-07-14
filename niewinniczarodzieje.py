#%% import 
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from datetime import datetime
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time

#%% def    

def get_article_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links


def extract_issue_links(archive_link):
    html_text = requests.get(archive_link).text
    soup = BeautifulSoup(html_text, 'html.parser')
    archive = soup.find('section', {'id': 'nav_menu-3'})
    links_and_issues = [{'Issue link': x.a.get('href'), 'Issue': x.text} for x in archive.find_all('li')]
    return links_and_issues



def information_from_archive(issue_link, issue):
    html_text = requests.get(issue_link).text
    soup = BeautifulSoup(html_text, 'html.parser')

    article_information = [x for x in soup.find_all('div', class_='card-content')]
    for x in article_information:
        dict_of_article = {'Link': x.find('a', class_='link').get('href'),
                           'Tytuł artykułu': x.find('a', class_='link').text,
                           'Tagi': " | ".join([x.text for x in x.find_all('a', class_='entry-tax-item mr-2 last:mr-0')]),
                           'Numer': issue}
        all_article_information_from_archive.append(dict_of_article)
    return all_article_information_from_archive


def dictionary_of_article(article_link):  
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    response = requests.get(article_link, headers=headers, allow_redirects=False)
    response.encoding = 'utf-8'  # wymuś UTF-8
    
    while 'Error 503' in response:
        time.sleep(2)
        response = requests.get(article_link, headers=headers, allow_redirects=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # html_text = requests.get(article_link).text
    # while 'Error 503' in html_text:
    #     time.sleep(2)
    #     html_text = requests.get(article_link).text
    # soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        date_of_publication = soup.find('time', class_='published')['datetime'][:10]
    except:
        date_of_publication = None   
    
    try:
        author = soup.find('a', class_='entry-meta-link').text
    except:
        author = None
    
    
    try:
        title_of_article = soup.find('div', class_='card-content').find('h1').text
    except: 
        title_of_article = None

    article = soup.find('div', class_='yuki-article-content yuki-entry-content clearfix mx-auto prose prose-yuki')
    
    try:
        text_of_article = " ".join([x.text.strip().replace('\xa0', ' ') for x in article.find_all('p')])
    except:
        text_of_article = None
    
    tags = None
    issue = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'niewinni-czarodzieje', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Numer': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if article and article.find_all('img') else False,
                             'Filmy': True if article and article.find_all('iframe') else False,
                             'Linki do zdjęć': photos_links}

    all_results.append(dictionary_of_article)
    
    
 
#%% main
article_links = get_article_links('https://niewinni-czarodzieje.pl/post-sitemap.xml')
   
links_and_issues = extract_issue_links('https://niewinni-czarodzieje.pl/')

all_article_information_from_archive = []
for x in tqdm(links_and_issues): 
    information_from_archive(x.get('Issue link'), x.get('Issue'))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, article_links),total=len(article_links)))
   

#enrichment all_results 

for x in all_results: 
    if x['Tagi'] == None: 
        for y in all_article_information_from_archive:
            if y['Link'] == x['Link']:
                x['Tagi'] = y['Tagi']
                x['Numer'] = y['Numer']


   
df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/niewinniczarodzieje_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/niewinniczarodzieje_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    