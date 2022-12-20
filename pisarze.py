#%% import 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from functions import date_change_format_short
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%def


def get_links_from_sitemap_posts(link):   
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    post_sitemap_links = [e.text for e in soup.find_all('loc') if re.match(r'(https\:\/\/pisarze\.pl\/)(post-sitemap.*)', e.text)]
    return post_sitemap_links

def get_posts_links(link): 
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    articles_links = [e.text for e in soup.find_all('loc')]
    all_articles_links.extend(articles_links)

































#%% main

sitemap_link = 'https://pisarze.pl/sitemap_index.xml'

post_sitemap_links = get_links_from_sitemap_posts('https://pisarze.pl/sitemap_index.xml')


all_articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_posts_links, post_sitemap_links),total=len(post_sitemap_links)))





#Dwutygodnik. Numery są dostępne tutaj: https://pisarze.pl/poprzednie-numery/ 


















