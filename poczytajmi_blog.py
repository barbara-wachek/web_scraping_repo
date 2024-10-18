#%%import
from __future__ import unicode_literals
import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import time


#%% def
def get_articles_links(sitemap_link):   
    html_text_sitemap = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    sitemap_links = [e.text for e in soup.find_all('loc') if re.match(r'https:\/\/poczytajmi\.blog\/20', e.text)]
    articles_links.extend(sitemap_links)  
    return articles_links


def dictionary_of_article(article_link):
    # article_link = 'https://poczytajmi.blog/2021/01/15/byla-raz-starsza-pani/'
    # article_link = 'https://poczytajmi.blog/2023/05/24/gra-o-spadek/'
    # article_link = 'https://poczytajmi.blog/2021/06/23/irenka-dziewczynka-z-wilna/'
    # article_link = 'https://poczytajmi.blog/2020/03/21/trzynastka-na-karku/' #dwie książki w opisie książki
    # article_link = 'https://poczytajmi.blog/2020/02/12/mali-bohaterowie/' #dwie książki
    # article_link = 'https://poczytajmi.blog/2020/03/02/dopoki-niebo-nie-placze/' #kilku autorów jednej ksiazki
    # article_link = 'https://poczytajmi.blog/2020/02/16/czarodzieje-wyobrazni-portrety-polskich-ilustratorow/'
    # article_link = 'https://poczytajmi.blog/2020/03/18/dawca/'
    # article_link = 'https://poczytajmi.blog/2019/11/20/dziecko-czarownicy-spadkobierczyni/'
    # article_link = 'https://poczytajmi.blog/2020/01/02/mama-zniosla-jajko/' #DWIE KSIAZKI DWOCH ROZNYCH AUTORÓW


    
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        author = soup.find('a', class_='url fn n').text
    except(TypeError, AttributeError):
        author = None
        
    try:
        title_of_article = soup.find('h1', class_='entry-title').text.strip()
    except AttributeError:
        title_of_article = None
        
    
    try:
        date_of_publication = soup.find('time', class_='entry-date published')['datetime']
        date_of_publication = re.search(r'\d{4}-\d{2}-\d{2}', date_of_publication).group(0)
    except TypeError:
        try:
            date_of_publication = soup.find('time', class_="entry-date published updated")['datetime']
            date_of_publication = re.search(r'\d{4}-\d{2}-\d{2}', date_of_publication).group(0)
        except TypeError:
            date_of_publication = None
    except AttributeError:
        date_of_publication = None
    
    
    
    try:
        article = soup.find('div', class_='entry-content')
    except AttributeError:
        article = None
        
    try:    
        text_of_article = " ".join([x.text.strip() for x in article.find_all('p')])
    except AttributeError:
        text_of_article = None
    
    try:
        tags = " | ".join([x.text for x in soup.find('span', class_='tags-links').find_all('a')])
    except AttributeError:
        tags = None
        
    try:
        book_description = " ".join([x.text for x in article.find_all('p') if re.match(r'.*\„(.*)\d{4}$', x.text)])
    except (AttributeError, KeyError, IndexError, TypeError):
        book_description = None  
        

        
    try:   
        first_quotation_mark = re.search(r'\„', book_description).span()
        author_of_book = book_description[:int(first_quotation_mark[0])].strip()
    except (AttributeError, KeyError, IndexError, TypeError):
        author_of_book = None  
        
        
    # try:    
    #     title_of_book = re.search(r'\„.*\”', book_description).group(0)
    # except (AttributeError, KeyError, IndexError, TypeError):
    #     title_of_book = None   
        
    
    try:    
        title_of_book = " | ".join([x.group(0) for x in re.finditer(r'\„[\p{L}\,\s\?\!\.]*\”', book_description)])
    except (AttributeError, KeyError, IndexError, TypeError):
        title_of_book = None   
        
        
    if " | " in title_of_book:    #Jesli jest kilka tytułów książki tzn. ze mamy dwie pozycje, a wiec trzeba sprawdzic, czy nie nalezy dopisac kogos do autorów
        if len([x.group(0) for x in re.finditer(author_of_book, book_description)]) < 2:   #Sprawdzenie, czy juz znaleziony autor sie powtarza w opisie. Jesli tak, to odpuszczamy dalsza analize, bo to najprawodopodniej dwie ksiazki (dwa tytuly) jednego autora
            try:
                first_book_year_span = [x for x in re.finditer(r'\d{4}', book_description)][0].span()[1]
                end_of_author = [x for x in re.finditer(r'\„', book_description)][1].span()[0]
                second_author = book_description[int(first_book_year_span):int(end_of_author)].strip()
                author_of_book = author_of_book + " | " + second_author
            except (AttributeError, KeyError, IndexError, TypeError):
                author_of_book = None
        
    try:
        publisher = " | ".join(re.findall(r'wyd\.\:\s[\p{L}\s\-]*', book_description))
    except (AttributeError, KeyError, IndexError, TypeError):
        publisher = None
    
    try:
        place_and_year = " | ".join([x.strip() for x in re.findall(r'\p{L}*\s?-?\p{L}*\s\d{4}', book_description)])
    except (AttributeError, KeyError, IndexError, TypeError):
        place_and_year = None
        
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a', href=True)] if not re.findall(r'poczytajmi', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
    
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img', src=True)])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None

    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Opis książki': book_description, 
                             'Autor książki': author_of_book, 
                             'Tytuł książki': title_of_book,
                             'Wydawnictwo': publisher, 
                             'Rok i miejsce wydania': place_and_year, 
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links}
        
            
    all_results.append(dictionary_of_article)

#%% main
articles_links = []
sitemap_link = get_articles_links('https://poczytajmi.blog/sitemap.xml')


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)


with open(f'data\\poczytajmi_blog_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   

with pd.ExcelWriter(f"data\\poczytajmi_blog_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



