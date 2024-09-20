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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#%% def
def get_sitemap_links(sitemap):
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    return links

def dictionary_of_article(article_link):
    # article_link = 'https://odystopiach.blogspot.com/2024/01/inowrocawski-ratusz-z-pegasusem-w-tle.html'
    # article_link = 'https://odystopiach.blogspot.com/2023/10/piaty-krag-pieka.html'
    # article_link = 'https://odystopiach.blogspot.com/2023/10/program-pegasus-historia-upadku.html'
    # article_link = 'https://odystopiach.blogspot.com/2016/11/uniwersum-agiernika.html' #inne style w tekscie artykulu
    # article_link = 'https://odystopiach.blogspot.com/2022/03/kosmiczny-wyrzut-sumienia.html' #brak ISBN
    # article_link = 'https://odystopiach.blogspot.com/2016/11/bez-gebokosci.html' #bez tyt. oryginału
    # article_link = 'https://odystopiach.blogspot.com/2011/11/biochemia-przerazenia.html' #tytuł
    # article_link = 'https://odystopiach.blogspot.com/2017/03/wycieczka-pana-brouczka-na-ksiezyc.html' #tekst innego autora! 
    # article_link = 'https://odystopiach.blogspot.com/2022/03/anarchistyczne-zaswiaty-rownolege.html'
    # article_link = 'https://odystopiach.blogspot.com/2022/08/nic-sie-nie-dzieje-wszyscy-sie-nudza.html'
    # article_link = 'https://odystopiach.blogspot.com/2017/10/gdy-rozum-spi-bola-mnie-zeby.html'
    # article_link = 'https://odystopiach.blogspot.com/2011/09/metoda-patchworku.html' #autorka
    # article_link = 'https://odystopiach.blogspot.com/2016/01/jesli-nie-soma-to-co.html'
    # article_link = 'https://odystopiach.blogspot.com/2013/11/powiesc-niewyrazna.html'
    # article_link = 'https://odystopiach.blogspot.com/2022/08/nic-sie-nie-dzieje-wszyscy-sie-nudza.html'
    # article_link = 'https://odystopiach.blogspot.com/2023/02/przechytrzyc-chaos.html' #ISBN z X na koncu
    # article_link = 'https://odystopiach.blogspot.com/2016/01/wycinanie-swiata.html'
    # # article_link = 'https://odystopiach.blogspot.com/2020/04/stolica-wykolejencow.html'
    # # article_link = 'https://odystopiach.blogspot.com/2023/10/modelowy-odbiorca-nie-istnieje.html'
    # # article_link = 'https://odystopiach.blogspot.com/2023/10/modelowy-odbiorca-nie-istnieje.html'
    # # article_link = 'https://odystopiach.blogspot.com/2011/02/komedia-i-antyutopijny-sztafaz.html'
    # # article_link = 'https://odystopiach.blogspot.com/2024/01/problemy-cyfrowych-tubylczyn-i-tubylcow.html'
    # article_link = 'https://odystopiach.blogspot.com/2013/09/fabryka-faszow-i-przekrecania-mozgow.html'
    # article_link = 'https://odystopiach.blogspot.com/2023/02/opusci-mezczyzna-swoja-matke-i-poaczy.html'
    # article_link = 'https://odystopiach.blogspot.com/2015/09/technoremedium.html'
    # article_link = 'https://odystopiach.blogspot.com/2023/05/zonierz-i-niepanna.html'
    
    
    options = webdriver.ChromeOptions()
    #Poniższy wiersz kodu wyłącza wyskakujace okno z wyborem domyślnej przeglądarki!
    options.add_argument("--disable-search-engine-choice-screen")
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(article_link)
    #Poniższy kod czeka na pojawienie się sekcji o autorstwie (aż wczyta się wyskakujące okno)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'publish-info')))
        
    html_text = driver.page_source 
    soup = BeautifulSoup(html_text, 'html.parser')
     
    author = soup.find('a', class_='url fn').text
    
    try:
        title_of_article = soup.find('h1', class_='title entry-title').text.strip()
    except AttributeError:
        title_of_article = None
  
    date_of_publication = soup.find('abbr', class_='time published')['title']
    date_of_publication = re.findall(r'\d{4}-\d{2}-\d{2}', date_of_publication)[0]
     
    article = soup.find('div', class_='article-content entry-content')
    text_of_article = article.text.strip()
   
    try:
        author_of_book = re.findall(r'(?<=autorz?y?k?a?:\s).*(?=\n*tytuł.*)', text_of_article)[0].strip()
    except IndexError:
        author_of_book = None 

    try:
        title_of_original_book = re.findall(r'(?<=tytuł oryginału:)\s?.*\n?\n?(?=przekład.*)', text_of_article)[0].strip()
    except IndexError:
        title_of_original_book = None
        
    try:
        title_of_book = re.findall(r'(?<=tytuł:)\s?.*\n?(?=\n*.*)', text_of_article)[0].strip()
    except IndexError:
        title_of_book = None
        
    if title_of_book == None:
        try:
            title_of_book = "Dotyczy dzieła: " + [x.text for x in article.find_all('b') if x.text.startswith('o ')][0]
        except (AttributeError, IndexError):
            title_of_book = None
       
    try:
        translator = re.findall(r'(?<=przekład:)\s?.*\n?(?=\n*wydawnictwo.*)', text_of_article)[0].strip()
    except IndexError:
        translator = None
       
    try:
        publish_year = re.findall(r'(?<=rok wydania:)\s?\d{4}\n?(?=.*)', text_of_article)[0].strip()
    except IndexError:
        publish_year = None
            
    try: 
        ISBN = re.findall(r'(?<=ISBN:)[\s\d\-]*.?(?=rok)', text_of_article)[0].strip()
    except IndexError:
        ISBN = None
    
    if ISBN == None:
        try: 
            ISBN = re.findall(r'(?<=ISBN:)[\s\d\-]*.?(?=liczba)', text_of_article)[0].strip()
        except IndexError:
            ISBN = None
             
    try:
        publisher = re.findall(r'(?<=wydawnictwo:)\s?.*\n?(?=\n*ISBN.*)', text_of_article)[0].strip()
    except IndexError:
        publisher = None
        
    try:
        tags = " | ".join([x.text for x in soup.find_all('a', class_='label')])
    except IndexError:
        tags = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in article.find_all('a')] if not re.findall(r'blogger|blogspot|wcieniuskrzydel', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None
         
    try: 
        photos_links = ' | '.join([x['src'] for x in article.find_all('img')])  
    except (AttributeError, KeyError, IndexError):
        photos_links = None
    
    try:
        source = re.search(r'źródło:', text_of_article)[0]
    except TypeError:
        source = None
    
    if source == None:
        try: 
            source = re.search('Artykuł opublikowany pierwotnie', text_of_article)[0]
        except TypeError:
            source = None
        
    
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tagi': tags,
                             'Autor książki': author_of_book,
                             'Tytuł książki': title_of_book,
                             'Tytuł oryginału książki': title_of_original_book,
                             'Autor przekładu': translator,
                             'Rok wydania': publish_year,
                             'Wydawnictwo': publisher,
                             'ISBN': ISBN,
                             'Uwagi': 'Uwaga! Możliwy "przedruk"' if source != None else None,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': False if external_links == '' else external_links,
                             'Zdjęcia/Grafika': True if photos_links != None else False,
                             'Linki do zdjęć': photos_links
                             }
            
    all_results.append(dictionary_of_article)

#%% main
articles_links = get_sitemap_links('https://odystopiach.blogspot.com/sitemap.xml')    

all_results = []
with ThreadPoolExecutor(max_workers=3) as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))

    
with open(f'data\\odystopiach_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False)   


df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
# df['Tytuł książki'].notnull().sum() #153
# df['Autor książki'].notnull().sum() #107
# df['Tytuł książki'].str.contains(r'Dotyczy dzieła: .*').sum() #109
# df['ISBN'].notnull().sum()  #79 #118 po dodaniu jednego if #107 #119
# df['Wydawnictwo'].notnull().sum() #125
# df['Tekst artykułu'].notnull().sum()


with pd.ExcelWriter(f"data\\odystopiach_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
    



