# -*- coding: utf-8 -*-
"""mariuszsieniewicz

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1sPVgkjufCc23yWHk93LWYHpS55r3SJl9
"""

!pip install xlsxwriter

#%% import
from __future__ import unicode_literals
import regex as re
import time
from datetime import datetime
from time import mktime
import requests
from bs4 import BeautifulSoup
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
import xlsxwriter


#%% functions

#Funkcja zmieniąjąca format daty z "12 października 2002" na "2002-10-12"
def date_change_format_short(date_of_publication):
    date = re.sub(r'(.*\s)(\d{1,2}\s)(.*)(\s\d{4})(\—\s[\w]*\s[\w]*\s[\w]*)', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)

    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))
    new_date = format(changed_date.date())
    return new_date

#Funkcja zmieniąjąca format daty z "wtorek, 12 października 2002" na "2002-10-12"
def date_change_format_long(date_of_publication):
    date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)

    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))
    new_date = format(changed_date.date())
    return new_date

#Funkcja do scrapowania linków z podanej strony sitemap
def get_links(sitemap_link):
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def get_sitemap_links(sitemap_link):
    # sitemap_link = 'http://mariuszsieniewicz.blogspot.com/sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc')]
    return links

def get_links_from_sitemap(link):
    # link = sitemap_links[0]
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_pages = [e.text for e in soup.find_all('loc')]
    articles_links.extend(links_pages)

def dictionary_of_article(article_link):

    r = requests.get(article_link)
    html_text = r.text
    while '429 Too Many Requests' in html_text:
        time.sleep(5)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    try:
      date_of_publication = soup.find('h2', class_='date-header').text
      date_of_publication = date_change_format_long(date_of_publication)
    except:
      date_of_publication = "no date"
    # category = soup.find('a', {'rel': 'category tag'}).text
    text_of_article = soup.find('div', class_='post-body entry-content')
    article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    #author = soup.find('a', class_='g-profile').text.strip()
    try:
        title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
        tags = '|'.join(set([e.text for e in soup.find_all('a', {'rel': 'tag'})]))

        try:
            external_links = ' | '.join([x for x in [x['href'] for x in text_of_article.find_all('a')] if not re.findall(r'blogspot', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None

        try:
            photos_links = ' | '.join([x['src'] for x in text_of_article.find_all('img')])
        except (AttributeError, KeyError, IndexError):
            photos_links = None

        dictionary_of_article = {"Link": article_link,
                                 "Data publikacji": date_of_publication,
                                 "Tytuł artykułu": title_of_article.replace('\xa0', ' '),
                                 "Tekst artykułu": article,
                                 "Autor": "Mariusz Szaniewicz",
                                 "Tagi": tags,
                                 # "Kategoria": category,
                                 'Linki zewnętrzne': external_links,
                                 'Zdjęcia/Grafika': True if [x['src'] for x in text_of_article.find_all('img') if 'src' in x.attrs] else False,
                                 'Filmy': True if [x['src'] for x in text_of_article.find_all('iframe')] else False,
                                 'Linki do zdjęć': photos_links
                                 }

        all_results.append(dictionary_of_article)
    except AttributeError:
        errors.append(article_link)

sitemap_links = get_sitemap_links("http://mariuszsieniewicz.blogspot.com/sitemap.xml")
all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, sitemap_links),total=len(sitemap_links)))
with open(f'mariuszsieniewicz{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)
df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
with pd.ExcelWriter(f"mariuszsieniewicz{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
    df.to_excel(writer, 'Posts', index=False)

df