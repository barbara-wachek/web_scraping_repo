#%% import
from dataclasses import dataclass, asdict
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_long, get_links
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%dataclass
@dataclass
class Record: 
    Link: str
    Date_of_publication: str
    Author: str
    Title_of_article: str
    Text_of_article: str
    Tags: str
    External_links: str
    Photos_links: str
    Article_comments: str


#%%def

def get_article_pages(link):   
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc')]
    articles_links.extend(sitemap_links)   


def dictionary_of_article(link): 
    link = 'https://krzysztofjaworski.blogspot.com/2016/03/sobota-z-poezja-xix.html'
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    author = soup.find('a', class_='g-profile')
    if author:
        author = author.span.text
    else:
        author = None
    
    title_of_article = soup.find('h3', class_='post-title entry-title')
    if title_of_article:
        title_of_article = title_of_article.text.strip()
    else:
        title_of_article = None


    date_of_publication = soup.find('h2', class_='date-header')
    if date_of_publication:
        new_date = date_change_format_long(date_of_publication.text)
    else:
        new_date = None
    
    content_of_article = soup.find('div', class_='post-body entry-content')
    
    if content_of_article:
        text_of_article = content_of_article.text
        if text_of_article:
            text_of_article = content_of_article.text.strip().replace('\n', '').replace('\xa0', '')
        else:
            text_of_article = None
    else:
        text_of_article = None
        
   
    tags = soup.find('span', class_='post-labels')
    if tags:
        tags = " | ".join([x.text for x in soup.find('span', class_='post-labels').findChildren('a')])
    else:
        tags = None

   comments = soup.find('div', class_='comments-content')
    if comments:
        comments_authors = [x.text for x in comments.find_all('cite', class_='user')]
        list_of_comments = [x.text for x in comments.find_all('p', class_='comment-content')]
       
        
        article_comments = []        
        for a in comments_authors:
            for c in list_of_comments:
                author_and_comment = f'{a}: {c}'
                article_comments.append(author_and_comment)
                
        article_comments = " | ".join(article_comments)
        
    else:
        article_comments = None
   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'blogger|blogspot|krzysztofjaworski', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    new_record = Record(Link=link,
        Date_of_publication=new_date,
        Author=author,
        Title_of_article=title_of_article,
        Text_of_article=text_of_article,
        Tags=tags,
        External_links=external_links,
        Photos_links=photos_links,
        Article_comments=article_comments
        )
      
    all_results.append(asdict(new_record))

#%%main

sitemap_links = get_links('https://krzysztofjaworski.blogspot.com/sitemap.xml')

articles_links = []    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_pages, sitemap_links),total=len(sitemap_links)))


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Date_of_publication', ascending=False)

with open(f'krzysztof_jaworski_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

   
with pd.ExcelWriter(f"krzysztof_jaworski_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"krzysztof_jaworski_{datetime.today().date()}.xlsx", f'krzysztof_jaworski_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  











