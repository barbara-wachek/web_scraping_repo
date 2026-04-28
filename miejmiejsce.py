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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import xlsxwriter


#%% functions

#Funkcja zmieniąjąca format daty z "12 października 2002" na "2002-10-12"
def date_change_format_short(date_of_publication):
    lookup_table = {
        "stycznia": "01", "lutego": "02", "marca": "03",
        "kwietnia": "04", "maja": "05", "czerwca": "06",
        "lipca": "07", "sierpnia": "08", "września": "09",
        "października": "10", "listopada": "11", "grudnia": "12"
    }

    # usuń przecinek
    date_of_publication = date_of_publication.replace(",", "").lower()

    parts = date_of_publication.split()

    month = lookup_table[parts[1]]
    day = parts[0]
    year = parts[2]

    return f"{year}-{month}-{int(day):02d}"

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
def get_article_links(url):
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'lxml')    
    links = [(e.find('a')['href'],
              e.find('h2', class_='title').text.strip(),
              e.find('div', class_='name').text.strip(),
              e.find('div', class_='name').find_next('div').text.strip(),
              e.find('p').text.strip(),
              ) 
              for e in soup.find_all('div', class_='col-sm-6')]
    return links

def dictionary_of_article(article_tuple):
    article_link = 'https://miejmiejsce.com' + article_tuple[0]
    title_of_article = article_tuple[1]
    author = article_tuple[2]
    category = article_tuple[3]
    description = article_tuple[4]

    r = requests.get(article_link, timeout=20)
    r.raise_for_status()
    html_text = r.text
    soup = BeautifulSoup(html_text, 'lxml')

    try:
      date_of_publication = soup.find_all(lambda tag: tag.name == "div" and tag.text.startswith('20'))[0].text
    except:
      date_of_publication = ""

    try:
      issue = soup.find_all(lambda tag: tag.name == "b" and tag.text.startswith('Numer:'))[0].text
    except:
      issue = ""

    try:
        text_of_article = soup.find('div', class_='article-text')
        article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    except (AttributeError, KeyError, IndexError):
        article = ""
    
    try:
        tags = '|'.join(set([e.text for e in soup.find_all('a', {'rel': 'tag'})]))
    except (AttributeError, KeyError, IndexError):
        tags = ""

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
                            "Autor": author,
                            "Kategoria": category,
                            "Opis": description,
                            "Tagi": tags,
                            'Linki zewnętrzne': external_links,
                            'Zdjęcia/Grafika': True if photos_links else False,
                            'Linki do zdjęć': photos_links
                            }

    return dictionary_of_article

#%%
base_links = [
    'https://miejmiejsce.com/literatura/more?page=0',
    'https://miejmiejsce.com/literatura/more?page=8',
    'https://miejmiejsce.com/literatura/more?page=16',
    'https://miejmiejsce.com/teatr/more?page=0',
    'https://miejmiejsce.com/teatr/more?page=8',
    'https://miejmiejsce.com/teatr/more?page=16',
    'https://miejmiejsce.com/teatr/more?page=24',
    'https://miejmiejsce.com/teatr/more?page=32',
    'https://miejmiejsce.com/teatr/more?page=40',
    'https://miejmiejsce.com/teatr/more?page=48',
]

article_tuples = []
for link in base_links:
    tuples = get_article_links(link)
    article_tuples.extend(tuples)

#%%
all_results = []
errors = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(dictionary_of_article, tup) for tup in article_tuples]

    for future in tqdm(as_completed(futures), total=len(futures)):
        try:
            result = future.result()
            if result:
                all_results.append(result)
        except Exception as e:
            errors.append(str(e))

#%%
len(all_results)

#%%
with open(f'data/miejmiejsce_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)
df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
with pd.ExcelWriter(f"data/miejmiejsce_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
    df.to_excel(writer, 'Posts', index=False)
