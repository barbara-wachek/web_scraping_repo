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
def get_category_links(home_page):
    html_text = requests.get(home_page).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('li', class_='nav__item')]
    categories = [e.text.strip() for e in soup.find_all('li', class_='nav__item')]
    dictionary_of_category_links = {categories[e]: links[e] for e in range(len(links))}

    return dictionary_of_category_links
       
#Funkcja rekurencyjna
def get_articles_links(category_link):
    format_link = 'https://kultura.gazeta.pl'
    html_text = requests.get(category_link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [e.a['href'] for e in soup.find_all('article', class_='article')]
    articles_links.extend(links)
    
    try:
        if soup.find('a', class_='next')['href']:
            next_page = format_link + soup.find('a', class_='next')['href']
            return get_articles_links(next_page)
    except TypeError:
        return articles_links
      

def dictionary_of_article(article_link):
    #Rezygnacja z pobierania linkow i zdjec, bo tego jest duzo i nie sa zwiazane z artykulem. Jest od groma reklam i odnosnikow
    # article_link = 'https://kultura.gazeta.pl/kultura/7,127222,31321567,marta-piasecka-nowa-prowadzaca-wiadomosci-w-wpolsce24.html'
    # article_link = 'https://kultura.gazeta.pl/kultura/7,127222,31317522,w-nowych-odcinkach-m-jak-milosc-bedzie-sie-dzialo-do-obsady.html'
    # article_link = 'https://kultura.gazeta.pl/kultura/7,114528,31320808,j-k-rowling-nie-osiada-na-laurach-autorka-harry-ego-pottera.html'
    # article_link = 'https://kultura.gazeta.pl/kultura/7,114438,31109497,do-coming-outu-sprowokowal-go-kaczynski-raczek-wyznaje-najgorszy.html'
    
    html_text = requests.get(article_link).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(article_link).text 
    soup = BeautifulSoup(html_text, 'html.parser')
    
    try:
        author = soup.find('span', class_='article_author').text.strip()
    except AttributeError:
        author = None
        
    try:   
        date_of_publication = soup.find('span', class_='article_date').time['datetime']
        date_of_publication = re.findall(r'\d{4}-\d{2}-\d{2}', date_of_publication)[0]
    except AttributeError:
        date_of_publication = None
    
    try:    
        title_of_article = soup.find('h1', {'id':'article_title'}).text.strip()
    except AttributeError:
        title_of_article = None        
    
    # article_section = [e.text.strip() for e in soup.find_all('div', class_='bottom_section')] #Tu pobierał się cały tekst w tym reklamy
    
    #Do porownania z innymi linkami czy cala zawartosc artykulu jest pobrana. Moze jakies inne elementy pojawia sie w innych przykladach
    try:
        lead_section = [e.text for e in soup.find_all('div', {'id': 'gazeta_article_lead'})]
    except AttributeError:
        lead_section = NoneType
        
    try:    
        article = [e.text.strip() for e in soup.find_all(re.compile('p|h2|blockquote'), class_= re.compile('art_paragraph|art_sub_title|art_blockquote'))] 
    except AttributeError:
        article = None
    
    try:
        text_of_article = "\n".join(lead_section + article)
    except: 
        text_of_article = None
     
    try:    
        tags = " | ".join([e.text.strip() for e in soup.find_all('li', class_='tags_item')])
    except AttributeError: 
        tags = None

    try:
        category = soup.find('a', class_='nav__itemName nav__active').text
    except AttributeError:
        category = None
   
    dictionary_of_article = {'Link': article_link,
                             'Data publikacji': date_of_publication,
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Kategoria': category,
                             'Tagi': tags,
                             'Tekst artykułu': text_of_article
                             }
            
    all_results.append(dictionary_of_article)

#%% main

#Wywala kod na 22699 zapisie (błąd HTTP cos tam. Do zbadania). Czy w ogole pobierać kategorię quizy? itp.?

dictionary_of_category_links = get_category_links('https://kultura.gazeta.pl/kultura/0,0.html')    
category_links = [v for k,v in dictionary_of_category_links.items()]

articles_links = []
list(tqdm(map(get_articles_links, category_links),total=len(articles_links))) #Do zeskrobania wszystkich linków
#Wynik: ponad 55 tysiecy linków (ale z duplikatami)

# get_articles_links(category_links[0]) #Wiadomości (około 27 421)
# get_articles_links(category_links[1]) #Filmy (około 9 537)
# get_articles_links(category_links[2]) #Muzyka (około 5149)
# get_articles_links(category_links[3]) #Książki (około 1927)
# get_articles_links(category_links[4]) #TV i Seriale (około 7 514)
# get_articles_links(category_links[5]) #Sztuka (około 681)
# get_articles_links(category_links[6]) #Festiwale (około 175)
# get_articles_links(category_links[7]) #Quizy (około 2410)


#Odrzucić duplikaty (niektore linki mogą mieć kilka kategorii, wiec znajdowały się w kilku zbiorach). Różnica około 26 tysiecy

articles_links = list(dict.fromkeys(articles_links)) #30 455 (2024-10-31) #30 653 (2024-11-14)


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
 

#23168 zrobilo sie, a potem blad: SSLError: HTTPSConnectionPool(host='www.ciemnastronamiasta.gazeta.pl', port=443): Max retries exceeded with url: /ciemnastrona_kultura/7,166729,24091311,polskie-kryminaly-maja-wyjatkowy-klimat-nasze-miasta-doskonale.html?bo=1 (Caused by SSLError(CertificateError("hostname 'www.ciemnastronamiasta.gazeta.pl' doesn't match either of '*.gazeta.pl', 'gazeta.pl'")))       
 
all_results[23324]  
articles_links[23323]  


if 'https://kultura.gazeta.pl/kultura/7,114628,24101688,teatr-polonia-krystyny-jandy-swietuje-13-urodziny.html' in articles_links:
    print(articles_links.index('https://kultura.gazeta.pl/kultura/7,114628,24101688,teatr-polonia-krystyny-jandy-swietuje-13-urodziny.html'))
    last_scraped = articles_links.index('https://kultura.gazeta.pl/kultura/7,114628,24101688,teatr-polonia-krystyny-jandy-swietuje-13-urodziny.html')

articles_links_second_part = articles_links[last_scraped+1:]

#Zrobienie kopii all_results, która zawiera 23 tysiace rekordów:
    all_results_first_part = all_results


all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links_second_part),total=len(articles_links_second_part)))


#Scalenie dwóch list: 
final_all_results = all_results_first_part + all_results
 





with open(f'data\\kultura_gazetapl_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(final_all_results, f, ensure_ascii=False)   


df = pd.DataFrame(final_all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=True)
   
# df['Autor'].notna()

#Jeżeli przy jakimś rekordzie nie udalo sie pobrać daty, to albo link jest już nieaktywny, albo są to jakieś quizy lub materiały promocyjne. Najlepiej wyrzucić na etapie DF, żeby już nikt tego nie analizował niepotrzebnie. 


with pd.ExcelWriter(f"data\\kultura_gazetapl_{datetime.today().date()}.xlsx", engine='xlsxwriter') as writer:    
    df.to_excel(writer, 'Posts', index=False)   
    writer.save()     
   
    



