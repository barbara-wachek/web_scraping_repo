#%%import
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
from functions import date_change_format_long
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


#%%def

#BLOG

def pilipiuk_web_scraping_list_of_blog_pages(second_page):
    html_text = requests.get(second_page).text
    soup = BeautifulSoup(html_text, 'lxml')
    number_of_all_blog_pages = re.sub(r'(\/index\.php\/blog\?start\=)(\d+)', r'\2', soup.find('li', class_='pagination-end').a['href'])

    first_page_of_blog = 'https://www.pilipiuk.com/index.php/blog?limitstart=0' #Pierwszy link różni się budową od reszty, dlatego bedzie dodany jako pierwszy do listy (domyslnie)
    links_of_blog_pages = [first_page_of_blog]
    number = re.sub(r'(https:\/\/www\.pilipiuk\.com\/index\.php\/blog\?start\=)(\d+)', r'\2', second_page_of_blog)

    number = 10
    page_of_blog = 'https://www.pilipiuk.com/index.php/blog?start='

    while int(number) < int(number_of_all_blog_pages):
        links_of_blog_pages.append(page_of_blog + str(number))
        number = number + 10
    return links_of_blog_pages


def dictionary_of_article(link_of_blog_page):
    html_text = requests.get(link_of_blog_page).text
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link_of_blog_page).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    ten_articles_from_page =  soup.select('.items-row.cols-1') 
    

    for element in ten_articles_from_page: 
       
        date_of_publication = element.find('dd', class_='create').text.strip()
        date = re.sub(r'(.*\,\s)(\d{1,2})(\,)(.*)(\s\d{4})(\s\d{2}\:\d{2})', r'\2\4\5', date_of_publication).strip()
        lookup_table = {"styczeń": "01", "luty": "02", "marzec": "03", "kwiecień": "04", "maj": "05", "czerwiec": "06", "lipiec": "07", "sierpień": "08", "wrzesień": "09", "październik": "10", "listopad": "11", "grudzień": "12"}
        for k, v in lookup_table.items():
            date = date.replace(k, v)
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())
     
        text_of_article = ' '.join([x.text.replace('\xa0','') for x in element.find_all('p')])
        title_of_article = element.find('h2').text.strip()
        
        try:
            external_links = ' | '.join([x for x in [x['href'] for x in element.find_all('a')] if not re.findall(r'pilipiuk\.com', x)])
        except (AttributeError, KeyError, IndexError):
            external_links = None
            
        dictionary_of_article = {'Data publikacji': new_date,  #BRakuje linków do artykułów. Jakos trzeba do nich dotrzeć
                                 'Autor': 'Andrzej Pilipiuk',
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links}


        all_results.append(dictionary_of_article)


#AKTUALNOSCI
    
def pilipiuk_link_of_first_note(link):  #Aktualnosci
    link = 'https://www.pilipiuk.com/index.php'
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    first_note_end_of_link = soup.find('ul', class_='fc_leading').h2.a['href']
    format_link = 'https://www.pilipiuk.com'
    first_note_link = format_link + first_note_end_of_link
    
    return first_note_link

def pilipiuk_link_of_all_notes(first_note_link):
    notes_links = [first_note_link]
    html_text = requests.get(notes_links[-1]).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    next_link = ''.join([x['href'] for x in soup.find_all('a') if x.text == 'nast. »' ])
    format_link = 'https://www.pilipiuk.com'
    next_link = format_link + next_link
    
    notes_links.append(next_link)
    
    
    
    
    
    
    
    
    date_of_publication = #z linku
    







#Z CR




def get_pilipiuk_blog_links(digit_from_range):
# all_links = []
# for digit_from_range in tqdm(range(1,8001)):
    format_url = 'https://www.pilipiuk.com/index.php/blog/'
    working_url = f'{format_url}{digit_from_range}'
    html_text = requests.get(working_url)
    if html_text.status_code == 200:
        all_links.append(html_text.url)
        
all_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_pilipiuk_blog_links, range(1,8001)),total=len(range(1,8001))))
    
    
    
# Rozbudować w taki sposob, aby wszystkie status cody magazynowac z jednej liscie i sprawdzic     + inny content 
    








while 'Error 503' in html_text:
       time.sleep(2)
       html_text = requests.get(link_of_blog_page).text
   soup = BeautifulSoup(html_text, 'lxml')    












#%% main 
link = 'http://www.pilipiuk.com/'
links_of_blog_pages = pilipiuk_web_scraping_list_of_blog_pages('https://www.pilipiuk.com/index.php/blog?start=10')

all_results = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, links_of_blog_pages),total=len(links_of_blog_pages)))


with open(f'pilipiuk_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        
 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f"pilipiuk_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   

    


#Do zesrapowania: linki z sekcji Publicystyka i wpisy z Aktualnosci, O Autorze







