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
import functions as fun
import my_classes


#%%def

def szelest_kartek_web_scraping_table_of_contents_page(link):
    html_text_sitemap = requests.get(link).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links_posts_from_table_of_contents = [x['href'] for x in soup.find_all('a') if 'pl/20' in x['href']]
    titles_from_table_of_contents = [x.text.replace('\xa0', ' ') for x in soup.find_all('a') if 'pl/20' in x['href']]
    links_and_titles_of_books = list(zip(titles_from_table_of_contents, links_posts_from_table_of_contents))
    return links_and_titles_of_books

def dictionary_of_article(article_link):
    html_text = requests.get(article_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('time', class_= re.compile(r"entry-date")).text
    new_date = fun.date_change_format_short(date_of_publication)
    text_of_article = soup.find('div', class_='entry-content')
    article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
    author = soup.find('span', class_='author vcard').text
    title_of_article = soup.find('h1', class_='entry-title').text
    title_and_author_of_book = " ".join([x.replace('“','"').replace('”', '"').replace('„','"') for x,y in articles_links_and_titles if y==article_link])
    #book = [x.text.replace('\xa0', ' ') for x in text_of_article.find_all('p', attrs={'style': 'text-align: right;'})][-1]
    title_of_book = re.sub(r'(.*)(\".*\")', r'\2', title_and_author_of_book)
    author_of_book = re.sub(r'(.*)(\".*\")', r'\1', title_and_author_of_book).strip()
    links_in_article = [x['href'] for x in text_of_article.find_all('a')]
    list_of_video = [x['src'] for x in text_of_article.find_all('iframe')]
    links_of_images = [x['src'] for x in text_of_article.find_all('img')]
    
    dictionary_of_article = {}

    try:
        dictionary_of_article['Link'] = article_link
        dictionary_of_article['Data publikacji'] = new_date
        dictionary_of_article['Autor'] = author
        dictionary_of_article['Tytuł artykułu'] = title_of_article.replace('\xa0', ' ')
        dictionary_of_article['Tekst artykułu'] = article
        dictionary_of_article['Książka'] =  title_and_author_of_book
        dictionary_of_article['Autor książki'] = author_of_book
        dictionary_of_article['Tytuł książki'] = title_of_book
        dictionary_of_article['Linki zewnętrzne'] = ' | '.join([x for x in links_in_article if not re.findall(r'afront\.org', x)])
        list_of_images = [x['src'] for x in text_of_article.find_all('img')]
        if list_of_images != []:
            dictionary_of_article['Zdjęcia/Grafika'] = 'TAK'
        if list_of_video != []:
            dictionary_of_article['Filmy'] = 'TAK'
        dictionary_of_article['Linki do zdjęć'] = ' | '.join(links_of_images[1:])   
            
    except AttributeError:
        pass
    except KeyError:
        pass
    except IndexError:   
        pass
    

    all_results.append(dictionary_of_article)  
                



#%%main
articles_links = fun.get_links('https://szelestkartek.pl/wp-sitemap-posts-post-1.xml')
articles_links_and_titles = szelest_kartek_web_scraping_table_of_contents_page('https://szelestkartek.pl/spis-tresci/')

all_results = [] 
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))  

with open(f'szelest_kartek_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"])
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"szelest_kartek_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()    
    






























