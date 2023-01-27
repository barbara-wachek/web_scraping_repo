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



def dictionary_of_article(link):    

    #link = 'https://pisarze.pl/2017/08/07/waclaw-holewinski-mebluje-glowe-ksiazkami-33/'
    # link = 'https://pisarze.pl/2021/06/15/rafal-skapski-profesor-aleksander-krawczuk-zaczyna-setny-rok-zycia/'
    # link = 'https://pisarze.pl/2014/06/02/jozef-jasielski-rezyser/'
    #link = 'https://pisarze.pl/2011/06/13/roman-sliwonik-wdziecznosc-co-to-takiego/'
    #link = 'https://pisarze.pl/2022/06/14/zbyszek-ikona-kresowaty-niekonczaca-sie-bajka-o-prawdzie-marc-chagall-w-muzeum-narodowym/'
    #link = 'https://pisarze.pl/2018/08/06/andrzej-walter-tak-jestem-2/'
    # link = 'https://pisarze.pl/2018/06/25/andrzej-walter-powrot-szczesciarza/'
    # link = 'https://pisarze.pl/2018/03/29/ksiazki-pod-lupa-swiatla-malego-miasta/'
    # link = 'https://pisarze.pl/2018/03/05/andrzej-walter-dwa-narody-z-jedna-dusza/'
    # link = 'https://pisarze.pl/2010/12/22/leszek-zulinski-pocalunki-pamieci/'
    # link - 'https://pisarze.pl/2021/11/30/waclaw-holewinski-mebluje-glowe-ksiazkami-227/'
    # link = 'https://pisarze.pl/2021/11/30/waclaw-holewinski-mebluje-glowe-ksiazkami-227/'
    # link = 'https://pisarze.pl/2016/04/28/zygmunt-krzyzanowski-130-rocznica-urodzin-pisarza/' #Wiadomosci
    # link = 'https://pisarze.pl/2019/01/10/jaroslaw-iwaszkiewicz-%ef%bb%bf/'
    # link = 'https://pisarze.pl/2019/01/01/stanislaw-nyczaj-pod-moja-batuta/'
    # link = 'https://pisarze.pl/2018/12/24/stefan-jurkowski-5/'
    # link = 'https://pisarze.pl/2018/12/13/wiersz-dnia-pod-redakcja-anny-musz-agnieszka-zajdowicz/'
    # link = 'https://pisarze.pl/2018/11/29/wiersz-dnia-pod-redakcja-anny-musz/'
    # link = 'https://pisarze.pl/2014/02/25/anita-spychaj/'
    # link = 'https://pisarze.pl/2018/03/19/jacek-walczak-stare-morze-i-czlowiek/' #proza
    # link = 'https://pisarze.pl/2016/10/24/eugeniusz-kabatc-pierwsza-wojna-narodow-mlyn-nad-narewka/'
    # link = 'https://pisarze.pl/2016/07/18/jan-tulik-nie-nadejdzie-juz-zlo/' #recenzja
    # link = 'https://pisarze.pl/2018/09/03/zygmunt-janikowski-zew-natury/'
    
    html_text = requests.get(link).text
    while 'Error 503' in html_text:
        time.sleep(3)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    

    date_of_publication = soup.find('time', class_='entry-date updated td-module-date')
    if date_of_publication:
        new_date = date_change_format_short(date_of_publication.text)
    else:
        new_date = None
      
        
      
    category = [x.text for x in soup.find_all('li', class_='entry-category')]  
    if category != '':
        category = " | ".join([x.text for x in soup.find_all('li', class_='entry-category')])
    else:
        category = None
        
      
    
    title_of_article = soup.find('h1', class_='entry-title')
    if title_of_article:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    else:
        title_of_article = None

  

    #Artykuły z niektórych kategorii powinny mieć z góry ustalony author=None (np. galeria, może też wiadomosci)
    if title_of_article != None:
        author = re.search(r'(\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*)(\–|\:)(.*)', title_of_article)
        if author:
            author = re.sub(r'(\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*)(\–|\:)(.*)', r'\1', title_of_article).strip()
        else:
            author = None
    else:
        author = None
    
    
    content_of_article = soup.find('div', class_='td-post-content')
    
    
    if content_of_article:
        text_of_article = [x.text for x in content_of_article.find_all('p') if x.text != '\xa0']
        if text_of_article:
            text_of_article = " | ".join([x.text.replace('\xa0', '').replace('\n', '') for x in content_of_article.find_all('p') if x.text != '\xa0'])
        else:
            text_of_article = None
    else:
        text_of_article = None
        
    

    if 'Recenzje' in category:
        book_description = [x.text for x in content_of_article.find_all('p') if re.search(r'(str\.)|(ISBN)|(stron )|(Stron )', x.text)]
        if book_description != '':
            book_description = " | ".join([x.text for x in content_of_article.find_all('p') if re.search(r'(str\.)|(ISBN)|(stron )|(Stron )', x.text)])
        else:
            book_description = None
    else:
        book_description = None
        
    
    

    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'pisarze', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
        
    try: 
        photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    

    if category == 'Promowane nowości':
        author = None
      
    if category == 'Stare spotkania':
       author = None    

    if re.search(r'Rok \d{4}', category):
        year = re.search(r'(?<=Rok )\d{4}', category).group(0)
    else:
        year = None
    
    if re.search(r'Nr \d{1,3}', category):
        issue = re.search(r'(?<=Nr )\d{1,3}', category).group(0)
    else:
        issue = None
             
    if 'Wiersz' in category or 'Poezja' in category: 
        title_of_poem = re.search(r'(?<=\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*\–|\:)(.*)', title_of_article)
        if title_of_poem:
            title_of_poem = re.search(r'(?<=\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*\–|\:)(.*)', title_of_article).group(0).strip()
        else:
            title_of_poem = 'DO UZUPEŁNIENIA'
    else:
        title_of_poem = None
            
            
    if 'Proza' in category: 
        title_of_prose_text = re.search(r'(?<=\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*\–|\:)(.*)', title_of_article)
        if title_of_prose_text:
            title_of_prose_text = re.search(r'(?<=\p{Lu}[\p{L}\.\-\s]*\p{Lu}[\p{L}\.\-\s]*\p{Lu}?[\p{L}\.\-\s]*\–|\:)(.*)', title_of_article).group(0).strip()
        else:
            title_of_prose_text = 'DO UZUPEŁNIENIA'
    else: 
         title_of_prose_text = None
            
   
         
    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date,
                             'Autor': author,
                             'Kategoria': category,
                             'Numer czasopisma': issue,
                             'Rocznik': year,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Opis dzieła': book_description,
                             'Tytuł wiersza': title_of_poem,
                             'Tytuł utworu prozatorskiego': title_of_prose_text,
                             'Linki zewnętrzne': external_links,
                             'Linki do zdjęć': photos_links}
        

    all_results.append(dictionary_of_article)    
    
    
    

#%% main

post_sitemap_links = get_links_from_sitemap_posts('https://pisarze.pl/sitemap_index.xml')

all_articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_posts_links, post_sitemap_links),total=len(post_sitemap_links)))

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links),total=len(all_articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df = df.sort_values('Data publikacji', ascending=False)


with open(f"pisarze_{datetime.today().date()}.json", 'w', encoding='utf-8') as f:
    json.dump(all_results, f)     

with pd.ExcelWriter(f"pisarze_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()    


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"pisarze_{datetime.today().date()}.json", f"pisarze_{datetime.today().date()}.xlsx"]
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



















