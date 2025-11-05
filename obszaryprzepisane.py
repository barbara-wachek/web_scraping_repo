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


#%% def    

def get_article_links(sitemap_links):
    
    if isinstance(sitemap_links, str):  # jeśli to pojedynczy link
        sitemap_links = [sitemap_links]  # zamień na listę

    all_article_links = []
    for sitemap_link in sitemap_links:
        html_text = requests.get(sitemap_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.text for e in soup.find_all('loc')]
        all_article_links.extend(links)
        
    return all_article_links



def dictionary_of_article(article_link):
    
    article_link = 'http://obszaryprzepisane.com/rafal-kasprzyk-recenzja/'
    # article_link = 'http://obszaryprzepisane.com/tomasz-dalasinski-op10/'

    
    response = requests.get(article_link)

    while 'Error 503' in response.text:
        time.sleep(2)
        response = requests.get(article_link)
    
    # Wymuś poprawne kodowanie
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'lxml')
    
   
    try:
        title_of_article = soup.find('h6', class_='elementor-heading-title elementor-size-default').text.strip()
    except: 
        title_of_article = None
        
    try:
        author = re.match(r'^(.*?)\s*\|', title_of_article).group(1)
    except:
        author = None
    


    span_tag = soup.find('span', string="Tagi")

    try: 
        # Pobierz rodzica tego spana
        parent = span_tag.parent
        
        # Znajdź wszystkie elementy 'a' będące dziećmi tego samego rodzica
        tags = " | ".join([x.text for x in parent.find_all('a', recursive=False)])  # tylko bezpośrednie dzieci
      
    except:
        tags = None

    try:
        issue = re.match(r'^(Nr.*?)\s*\|', tags).group(1)
    except:
        issue = None

    
    article = soup.find('div', class_='elementor-text-editor elementor-clearfix')  
    
    if article:    
        result_lines = []
    
        for elem in article.children:
            # Pomijamy puste teksty czy spacje
            if getattr(elem, 'name', None) is None:
                continue
            
            if elem.name == 'h3':
                # Tytuł wiersza - tekst na osobnej linii
                title = elem.get_text(strip=True)
                result_lines.append(title)
                result_lines.append("")  # dodajemy pustą linię po tytule (opcjonalnie)
            elif elem.name == 'p':
                # Wiersz - tekst, gdzie <br/> zamieniamy na \n
                # a wewnątrz <em> też jest tekst
                # Metoda: zamieniamy <br> na \n, potem get_text()
                
                # Zamieniamy <br> na \n (w tym elemencie)
                for br in elem.find_all("br"):
                    br.replace_with("\n")
                
                # Pobieramy tekst z p (z wcięciami, nowymi liniami etc.)
                paragraph_text = elem.get_text()
                
                # Usuwamy nadmiarowe spacje na końcach linii i podwójne nowe linie
                paragraph_text = "\n".join(line.strip() for line in paragraph_text.splitlines())
                
                result_lines.append(paragraph_text)
                result_lines.append("")  # linia przerwy między paragrafami (opcjonalnie)
    
    # Łączymy wszystko w jeden tekst
    try:
        text_of_article = "\n".join(result_lines)
    except:
        text_of_article = None
    
        
    #Popraw branie tekstow. Pierwszy link nie zwraca tekstu w calosci. Zbadaj

        
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'obszaryprzepisane', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Numer czasopisma': issue,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tags': tags,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x.get('src') for x in article.find_all('img')] else False,
                             'Filmy': True if [x.get('src') for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)
    
    
 
#%% main
all_article_links = get_article_links('http://obszaryprzepisane.com/post-sitemap.xml')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_article_links),total=len(all_article_links)))
   

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)


with open(f'data/makiwgiverny_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)    

with pd.ExcelWriter(f"data/makiwgiverny_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)     
   
    


   
   
   
   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    