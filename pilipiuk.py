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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

#from my_classes import ScrapingReport

#%%def

def get_pilipiuk_blog_links(digit_from_range):
#all_links = []
#list_of_status_code= []
#for digit_from_range in tqdm(range(1,8001)):
    format_url = 'https://www.pilipiuk.com/index.php/blog/'
    working_url = f'{format_url}{digit_from_range}'
    html_text = requests.get(working_url)
    if html_text.status_code == 200:
        all_links.append(html_text.url)
        list_of_status_code.append(html_text.status_code)
    else: 
        list_of_status_code.append(html_text.status_code)


def dictionary_of_article(link):
    html_text = requests.get(link).text
# html_collect = []
# for link in tqdm(all_links):
#     #all_links[100] = link
#     html_text = str(requests.get(link).status_code)
#     html_collect.append(html_text)
  
    while 'Error 503' in html_text:
        time.sleep(2)
        html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    try:
        date_of_publication = soup.find('dd', class_='create').text.strip()
        date = re.sub(r'(.*\,\s)(\d{1,2})(\,)(.*)(\s\d{4})(\s\d{2}\:\d{2})', r'\2\4\5', date_of_publication).strip()
        lookup_table = {"styczeń": "01", "luty": "02", "marzec": "03", "kwiecień": "04", "maj": "05", "czerwiec": "06", "lipiec": "07", "sierpień": "08", "wrzesień": "09", "październik": "10", "listopad": "11", "grudzień": "12"}
        for k, v in lookup_table.items():
            date = date.replace(k, v)
        result = time.strptime(date, "%d %m %Y")
        changed_date = datetime.fromtimestamp(mktime(result))   
        new_date = format(changed_date.date())
    except (AttributeError, KeyError, IndexError):
        new_date = None
        
        
    content_of_article = soup.find('div', class_='item-page')
    text_of_article = " ".join([x.text.replace('\xa0','') for x in content_of_article.find_all('p')])
    
    try:
        title_of_article = soup.find('div', class_='item-page').h2.text.strip()
    except (AttributeError, KeyError, IndexError):
        title_of_article = None
    try:
        author = content_of_article.find('dd', class_='createdby').text.strip()
    except (AttributeError, KeyError, IndexError):
        author = None
    
    try:
        external_links = ' | '.join([x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(index\.php)|images', x)])
    except (AttributeError, KeyError, IndexError):
        external_links = None



    dictionary_of_article = {'Link': link,
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Linki zewnętrzne': external_links
                            }


    all_results.append(dictionary_of_article)


#%%

list_of_status_code= []        
all_links = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_pilipiuk_blog_links, range(1,8001)),total=len(range(1,8001))))
    
all_results = []    
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_links), total=len(all_links)))


#Sprawdzenie status codes     
series_list_of_status_code = pd.Series(list_of_status_code)    
series_list_of_status_code.value_counts()  # Pojawiaja się tylko kody: 200 i 404

    
with open(f'pilipiuk_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f) 

df = pd.DataFrame(all_results).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

#Uzupełnienie raportu
#report = ScrapingReport('https://www.pilipiuk.com', len(df), df['Data publikacji'].iloc[0])    
#report.create_scraping_report()


with pd.ExcelWriter(f"pilipiuk_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()  



#Sprawdzenie, czy wsród linkow znajdują się również wpisy z kategorii Aktualnoci: ODPOWIEDŹ: TAK, znajdują się. + podstrony z lewego menu. 
#df_test = df[df['Tytuł artykułu'].str.contains('12 listopada.')]



# Rozbudować w taki sposob, aby wszystkie status cody magazynowac z jednej liscie i sprawdzic     + inny content 
    


    
#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"pilipiuk_{datetime.today().date()}.xlsx", f'pilipiuk_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  



##Przygotować się z budowania klasy, jak dodawać podpola 















