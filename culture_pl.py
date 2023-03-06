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
#from pydrive.auth import GoogleAuth
#from pydrive.drive import GoogleDrive
import time
#import random
from time import mktime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.common.exceptions import TimeoutException


# #biblioteka 
# import requests_html

#%% def

def get_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    sitemap_links = [x.text for x in soup.find_all('loc')]
    
    return sitemap_links

def get_article_links_from_sitemap_links(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc')]
    all_articles_links.extend(links)
    return all_articles_links


# def dictionary_of_article(link): 
       
#random_links_from_only_articles = random.choices(only_articles, k=100)
#Zawartosc 50 linków pobiera w czasie około 8 minut. 100 w 16 minut. 8 tysiecy rekordów w 21 godzin... 12 tysięcy w około 32 godziny



all_results = []


for link in tqdm(first_artykul_category_list):
    
    chrome_options = Options()
    chrome_options.headless = True
    
    driver = webdriver.Chrome("C:\\Users\\PBL_Basia\\Desktop\\ChromeDriver\\chromedriver.exe", options=chrome_options)
    driver.implicitly_wait(5)
    driver.get(link)
    
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, 'lxml')


    date_of_publication = soup.find('div', class_='published')
    if date_of_publication: 
        date_of_publication = date_of_publication.text 
        date = re.sub(r'(Opublikowany\:\s)(\d{1,2}\s)(\p{L}*)(\s)(\d{4})', r'\2\3\4\5', date_of_publication).strip()
        lookup_table = {"sty": "01", "lut": "02", "mar": "03", "kwi": "04", "maj": "05", "cze": "06", "lip": "07", "sie": "08", "wrz": "09", "paź": "10", "lis": "11", "gru": "12"}
        for k, v in lookup_table.items(): 
            date = date.replace(k, v)
            
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())
           
    else:
        new_date = None #wpisy o tworcach i dzielach i nie maja dat
    
        
    author = soup.find('div', class_='author-name')
    if author:
        author = author.text
        author = re.sub(r'(Autor|Autorzy)(\:\s)(.*)', r'\3', author)
        
    else:
        author = None
    
    
    title_of_article = soup.find('h1', class_='title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
           
    category = re.sub(r'(https?\:\/\/culture\.pl\/)(pl\/)?(\w*)(\/.*)', r'\3', link)
    content_of_article = soup.find('div', class_='article-content')

    about_author = soup.findChildren('div', class_='group-text')
    if about_author:
        about_author = " | ".join([x.find('div', class_='description').text for x in about_author if x.find('div', class_='description')])
    else:
        about_author = None
        
        
    text_of_article = [x for x in content_of_article.find_all('div', class_='content')]
    if text_of_article:
        try:
            text_of_article = " | ".join([x.p.text.replace('\n', ' ') for x in content_of_article.find_all('div', class_='content')]).strip()
        except AttributeError:
            text_of_article = 'ERROR!'
    else:
        text_of_article = None

    tags = soup.find('div', class_='topic')
    if tags: 
        tags = ' | '.join([x.text.replace('#','') for x in soup.find('div', class_='topic')])
    else:
        tags = None
    

  

    dictionary_of_article = {'Link': link, 
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags
                             }

    all_results.append(dictionary_of_article)
    




#%%main

sitemap_links = get_sitemap_links('https://culture.pl/pl/sitemap.xml')    

all_articles_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_article_links_from_sitemap_links, sitemap_links), total=len(sitemap_links)))       


#Do wybrania ze zbioru artykułow tylko tych ktore maja oznaczenie kategorii jako artykul (inne kategorie to np. wydarzenia, tworca, dzielo, galeria, miejsce - tu mogą byc instytucje, node, wideo, wydarzenie). Po wstępnym rozeznaniu kategoria dzieło tez jest do pobrania - są tam recenzje i notki o utworach


artykul_category = []
for x in all_articles_links:
    if re.match(r'https\:\/\/culture\.pl\/pl\/artykul\/.*', x):
        artykul_category.append(x)


first_artykul_category_list = artykul_category[0:500]
second_artykul_category_list = artykul_category[501:1001]
third_artykul_category_list = artykul_category[1001:1501]
forth_artykul_category_list = artykul_category[1501:2001]
fifth_artykul_category_list = artykul_category[2001:2501]
sixth_artykul_category_list = artykul_category[2501:3001]
seventh_artykul_category_list = artykul_category[3001:3501]
eighth_artykul_category_list = artykul_category[3501:4001]
ninth_artykul_category_list = artykul_category[4001:4501]
tenth_artykul_category_list = artykul_category[4501:5001]

with open(f'culture_pl_artykuly_000-1000{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   
     

# df = pd.DataFrame(all_results).drop_duplicates()
# df = df.sort_values('Data publikacji', ascending=False)






#%% Pozostale listy z linkami: 
dzielo_category = []
for x in all_articles_links:
    if re.match(r'https\:\/\/culture\.pl\/pl\/dzielo\/.*', x):
        dzielo_category.append(x)

wydarzenie_category = []
for x in all_articles_links:
    if re.match(r'https\:\/\/culture\.pl\/pl\/dzielo\/.*', x):
        wydarzenie_category.append(x)




df = pd.DataFrame(all_results)



# Autor często jest zapisany jako Culture.pl, a niżej u dołu artykułu jest podany właciwy autor













#%% Podejscie z wykorzystaniem API Culture.pl
# link = 'https://api.culture.pl/en/api/node/article'


# import urllib3
# http = urllib3.PoolManager()
# r = http.request('GET', 'https://api.culture.pl/en/api/node/article')
# r.status
# 200
# r.data
# r.headers
# 'User-agent: *\nDisallow: /deny\n'


# import json
# r = http.request('GET', 'https://api.culture.pl/en/api/node/article')
# json_file = json.loads(r.data.decode('utf-8'))

# next_link = json_file['links']['next']





# # # from urllib2 import Request, urlopen

# # request = Request('https://api.culture.pl/en/api/node/article')

# # response_body = urlopen(request).read()
# # print response_body





# #Sprobowac stworzyc 1 json ze wszystkich artykułów z response
# #otrzymać pełną zwrotkę co daje API artykułów 

# #2023-02-20
# #Po pobraniu 4803 aliasów linków zwraca błąd, bo next_link kieruje do błednej strony
# #API nie pozwala pobrać wszystkich artykułów. ZOstanie jeszcze okolo 25 tysiecy 



# def get_links_from_api(link):     
#     response = requests.get(link).json() 
#     licznik = 0 
    
#     while response != None:
#         data = response['data']
#         for x in data:
#             alias_link = x['attributes']['path']['alias']
#             all_api_articles_alias.append(alias_link) 
#         licznik = licznik + 1
#         next_link = response['links']['next']['href']  
#         try:
#             response = requests.get(next_link).json()
#         except:
#             response = None
#             print('BŁĄD')
#             print(licznik)
#             print(next_link)

   
  
# all_api_articles_alias = []
# get_links_from_api('https://api.culture.pl/en/api/node/article')
        

# # test_list = []
# # for x in all_api_articles_alias: 
# #     if x not in test_list:
# #         test_list.append(x)


# def create_links(link):
#     created_link = 'https://culture.pl/pl' + link
#     list_of_created_link.append(created_link)
    

# list_of_created_link = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(create_links, all_api_articles_alias), total=len(all_api_articles_alias))

# #%% Proba stworzenia jsona z danymi z artykulow: 
  

# def dictionary_of_article(link):     
#     response = requests.get(link).json() 

#     while response != None:
#         dictionary_of_article = dict()
#         data = response['data']
#         for x in data:
#             link = 'https://culture.pl/pl' + str(x['attributes']['path']['alias'])
#             date_of_publication = x['attributes']['created']
#             #lead = x['attributes']['field_summary']['value']
#             title = x['attributes']['title']
            
#             dictionary_of_article = {'Link' : link,
#                                      'Data publikacji': date_of_publication,
#                                      'Tytuł': title}
                                    
#             all_results.append(dictionary_of_article)
            
#         next_link = response['links']['next']['href']  
#         try:
#             response = requests.get(next_link).json()
#         except:
#             response = None
#             print('BŁĄD')
#             print(next_link)

   

# all_results = []
# dictionary_of_article('https://api.culture.pl/en/api/node/article')

# df = pd.DataFrame(all_results)

# # with ThreadPoolExecutor() as excecutor:
# #     list(tqdm(excecutor.map(dictionary_of_article, all_api_articles_alias), total=len(all_api_articles_alias)))       


























