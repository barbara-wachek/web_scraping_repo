#%% import 
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
from difflib import SequenceMatcher

#%% def  

#Problem: 13 na 16 numerów jest w formie pdfów


def get_article_links(issue_link):
    issue_link = 'https://czynna.com/magazyn/numer-1-2020/'

    html_text = requests.get(issue_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

  

def normalize_text(text):
    # usuń niealfanumeryczne znaki i zamień na małe litery
    return re.sub(r'[^a-z0-9ąćęłńóśźż]+', ' ', text.lower()).strip()

def extract_title_by_link(soup, article_link):
    # pobierz fragment linku po ostatnim '/'
    link_fragment = article_link.rstrip('/').split('/')[-1]
    normalized_link = normalize_text(link_fragment)

    candidates = soup.find_all('h1')
    best_match = None
    best_ratio = 0

    for h in candidates:
        norm_h_text = normalize_text(h.text)
        ratio = SequenceMatcher(None, norm_h_text, normalized_link).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = h.text.strip()

    # jeśli podobieństwo jest dobre (np. >0.5), zwróć tytuł, inaczej None
    if best_ratio > 0.5:
        return best_match
    return None



def dictionary_of_article(article_link, category):

    # article_link = 'https://www.tlenliteracki.pl/feliks-trzymalko-cztery-wiersze'
    # category = 'poezja'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    title_of_article = extract_title_by_link(soup, article_link)
      
    try:
        author = title_of_article.split('–')[0].strip()
    except:
        author = None
    
    article = soup.find('div', class_='section-text body-text')
    
    try:
        text_of_article = article.text
    except:
        text_of_article = None
    
    try:
        issue = soup.find('a', class_='anchor-underline issue-link').text.strip().replace('#', '')
    except:
        issue = None
        
        
    try:
        title_of_poem = " | ".join([x.text for x in article.find_all('strong')])
    except:
        title_of_poem = None

    if title_of_poem == None or title_of_poem == '':
        try:
            title_of_poem = " | ".join([x.text for x in article.find_all('h2')])
        except:
            title_of_poem = None
        
    try:
        biograms = " | ".join([x.text for x in soup.find('div', class_='bios').find_all('p')])
    except:
        biograms = None

   
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'tlenliteracki', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None


    dictionary_of_article = {'Link': article_link,
                             'Numer': issue,
                             'Kategoria': category,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tytuł utworu': title_of_poem,
                             'Tekst artykułu': text_of_article,
                             'Tekst biogramu': biograms,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': bool([x.get('src') for x in article.find_all('img')]) if article else False,
                             'Filmy': bool([x.get('src') for x in article.find_all('iframe')]) if article else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main

issues_links = ['https://czynna.com/magazyn/numer-1-2020/', 'https://czynna.com/magazyn/numer-2-2020/', 'https://czynna.com/magazyn/numer-3-2020/', 'https://czynna.com/magazyn/numer-4-2020/', 'https://czynna.com/magazyn/numer-5-2021/', 'https://czynna.com/magazyn/numer-6-2021/', 'https://czynna.com/magazyn/numer-7-2021/', 'https://czynna.com/magazyn/numer-8-2021/', 'https://czynna.com/magazyn/numer-9-2022/', 'https://czynna.com/magazyn/numer-10-2022/', 'https://czynna.com/magazyn/numer-11-2022/', 'https://czynna.com/magazyn/numer-12-2022/', 'https://czynna.com/magazyn/numer-13-2023/', 'https://czynna.com/magazyn/numer-14-2024/', 'https://czynna.com/magazyn/numer-15-2025/', 'https://czynna.com/magazyn/numer-16-2025/']

article_links = get_article_links






all_article_links = []

for base_url in tqdm(category_links):
    category = re.match(r'https://www\.tlenliteracki\.pl/([^/]+)/', base_url).group(1)

    for page_number in range(1, 100):
        url = base_url + str(page_number)

        response = requests.get(url)
        if response.status_code == 404:
            break  # Koniec podstron dla tej kategorii

        soup = BeautifulSoup(response.text, 'lxml')
        links = soup.find_all('a', class_='card-link')

        for link in links:
            href = link.get('href')
            if href:
                full_url = 'https://www.tlenliteracki.pl' + href
                all_article_links.append({
                    'Link': full_url,
                    'Kategoria': category
                })

args = [(d['Link'], d['Kategoria']) for d in all_article_links]
 
all_results = []

with ThreadPoolExecutor() as executor:
    for result in tqdm(executor.map(lambda p: dictionary_of_article(*p), args), total=len(args)):
        if result:
            all_results.append(result)
   

df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Numer', ascending=True)

json_data = df.to_dict(orient='records')

with open(f'data/tlenliteracki_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/tlenliteracki_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    