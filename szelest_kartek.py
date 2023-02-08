#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
#import my_classes

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from functions import date_change_format_short


#%%def

def get_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links


def szelest_kartek_web_scraping_table_of_contents_page(link):
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_posts_from_table_of_contents = [x['href'] for x in soup.find_all('a') if 'pl/20' in x['href']]
    titles_from_table_of_contents = [x.text.replace('\xa0', ' ') for x in soup.find_all('a') if 'pl/20' in x['href']]
    links_and_titles_of_books = list(zip(titles_from_table_of_contents, links_posts_from_table_of_contents))
    return links_and_titles_of_books

def dictionary_of_article(article_link):
    
    #article_link = 'https://szelestkartek.pl/2012/03/01/mistrz-w-krzywym-zwierciadle-2/'
    #article_link = 'https://szelestkartek.pl/2015/07/17/dssor/'
    #article_link = 'https://szelestkartek.pl/2015/09/03/nie/'
    
    
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    
    date_of_publication = soup.find('time', class_= re.compile(r"entry-date"))
    if date_of_publication:
        date_of_publication = date_of_publication.text
        new_date = date_change_format_short(date_of_publication)
    else:
        new_date = None
        
    content_of_article = soup.find('div', class_='entry-content')
    text_of_article = content_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    
    
    author = soup.find('span', class_='author vcard')
    if author:
        author = author.text
    else:
        author = None
    
    second_author = [x.text for x in content_of_article.find_all('p') if re.search(r'^Autor:\s.*', x.text)]
    if second_author != '':
        second_author = " | ".join(second_author)
        second_author = re.sub(r'(Autor\:\s)(.*)', r'\2', second_author)
    else:
        second_author = None
    
        
    title_of_article = soup.find('h1', class_='entry-title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None


    title_and_author_of_book = " ".join([x.replace('“','"').replace('”', '"').replace('„','"') for x,y in articles_links_and_titles if y==article_link])
    if title_and_author_of_book != '':  
        title_of_book = re.sub(r'(.*)(\".*\")', r'\2', title_and_author_of_book)
        author_of_book = re.sub(r'(.*)(\".*\")', r'\1', title_and_author_of_book).strip()
    else: 
        title_of_book = None
        author_of_book = None
        
    
    if title_of_book == None:
        if content_of_article.find('p', attrs={'style':'text-align: right;'}):
            title_and_author_of_book = content_of_article.find('p', attrs={'style':'text-align: right;'}).text
            if re.search(r'(?<=[\p{L}\s\-\.\,])*(\„.*\”)', title_and_author_of_book):
                title_of_book = re.search(r'(?<=[\p{L}\s\-\.])*(\„.*\”)', title_and_author_of_book).group(0).strip()
            else:
                title_of_book = None
            
            if re.search(r'[\p{L}\s\-\.\,]*(?=\„)', title_and_author_of_book):
                author_of_book = re.search(r'[\p{L}\s\-\.]*(?=\„)', title_and_author_of_book).group(0).strip()
            else:
                author_of_book = None

            
     
    try:
        external_links = ' | '.join([x for x in [x.get('href') for x in content_of_article.find_all('a') if x.get('href') != None] if not re.findall(r'szelestkartek', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None

    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': new_date,
                             'Autor': f'{author} ({second_author})',
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Książka': title_and_author_of_book,
                             'Tytuł książki': title_of_book,
                             'Autor książki': author_of_book,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img') if not re.findall(r'(bookmark)|(email)|(twitter)|(facebook)', x['src'])] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }
    
    all_results.append(dictionary_of_article)  
                



#%%main
articles_links = get_links('https://szelestkartek.pl/wp-sitemap-posts-post-1.xml')
articles_links_and_titles = szelest_kartek_web_scraping_table_of_contents_page('https://szelestkartek.pl/spis-tresci/')

all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))  

with open(f'szelest_kartek_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False) 

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"szelest_kartek_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()    
    






























