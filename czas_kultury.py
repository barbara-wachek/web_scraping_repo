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

#%%

# link = 'https://czaskultury.pl/archiwum-tekstow/' == 'https://czaskultury.pl/archiwum-tekstow/page/1/'

# https://czaskultury.pl/wydarzenia/
# https://czaskultury.pl/reading-book-sitemap.xml #?
# https://czaskultury.pl/reading-book-sitemap2.xml

# https://czaskultury.pl/feuilleton-sitemap.xml

# sitemap = 'https://czaskultury.pl/sitemap_index.xml'


def number_of_pages_in_archive_of_texts(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    number_of_pages_in_archive_of_texts = soup.find_all('a', class_='page-numbers')[-2].text
    return number_of_pages_in_archive_of_texts

def czas_kultury_get_links_from_archive(digit_from_range):
    format_url = 'https://czaskultury.pl/archiwum-tekstow/page/'
    working_url = f'{format_url}{digit_from_range}'
    links_of_archive_pages.append(working_url)
    return links_of_archive_pages

def czas_kultury_web_scraping_links_from_archive(link_of_archive_page):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.a['href'] for e in soup.find_all('div', class_='post-box-component')]
    all_archive_links.extend(links)
    return all_archive_links


#Dodać archiwum numerów, bo nie ma ich w archiwum tekstów + scrapować wpisy 
def czas_kultury_get_links_from_article_archive(link): 
    #link = 'https://czaskultury.pl/rok/2021/'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    all_articles = [x.a['href'] for x in soup.find_all('div', class_="post-box-component")]
    all_articles_from_archive.extend(all_articles)
    return all_articles_from_archive

    
#%% main 

#Linki z Archiwum tekstów:
number_of_pages_in_archive_of_texts =  number_of_pages_in_archive_of_texts('https://czaskultury.pl/archiwum-tekstow/page/6/')

links_of_archive_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_get_links_from_archive, range(1, int(number_of_pages_in_archive_of_texts)+1)), total=len(range(1, int(number_of_pages_in_archive_of_texts)+1))))

all_archive_links = []  
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_web_scraping_links_from_archive, links_of_archive_pages), total=len(links_of_archive_pages)))


#Linki z Archiwum numerów:
links_of_archive_year = ['https://czaskultury.pl/rok/2022/', 'https://czaskultury.pl/rok/2021/']
all_articles_from_archive = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(czas_kultury_get_links_from_article_archive, links_of_archive_year), total=len(links_of_archive_year)))
all_articles_from_archive = list(set(all_articles_from_archive))

all_articles_links = all_archive_links + all_articles_from_archive

# print(len(all_articles_from_archive)==len(set(all_articles_from_archive))) #Lista zawiera zdublowane elementy
# set_all_articles_from_archive = set(all_articles_from_archive)
# list(set_all_articles_from_archive) 









#Sprawdz, czy ktorykolwiek z elementow all_archive_links zawiera fragment tekstu, ktory nazywa sie pijesz-ty-pije-ja


re.findall(r'pijesz\-ty\-pije\-ja', all_archive_links)

[re.findall(r'pijesz\-ty\-pije\-ja',x) for x in all_archive_links]


for element in all_archive_links:
    if re.findall(r'pijesz\-ty\-pije\-ja', element):
        print(True)

[x for x in all_archive_links if 'pijesz-ty-pije-ja' in x]

    
'pijesz-ty-pije-ja'in'https://czaskultury.pl/artykul/pijesz-ty-pije-ja/' 
    
    
    
    
    
    
    
    