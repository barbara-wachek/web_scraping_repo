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

#%%URLs

techsty_dict = {'1': {'numer': 'https://techsty.art.pl/magazyn/magazyn1.html',
                      'rok': 2003,
                      'artykuły': ['https://techsty.art.pl/magazyn/coover/coover.htm',
                                   'https://www.techsty.art.pl/magazyn/bernstein/b01.htm',
                                   'https://techsty.art.pl/magazyn/mochola/pol1.htm',
                                   'https://techsty.art.pl/magazyn/pisarski/p01.htm',
                                   'https://techsty.art.pl/magazyn/sikora/s01.htm',
                                   'https://techsty.art.pl/magazyn/stunza/01.htm',
                                   'https://techsty.art.pl/magazyn/net-art-sztuka-wobec-interaktywnosci.htm',
                                   'https://techsty.art.pl/magazyn/mochola/mph01.htm',
                                   'https://techsty.art.pl/magazyn/r1.htm',
                                   'https://techsty.art.pl/magazyn/sikora/bloody.htm',
                                   'https://techsty.art.pl/magazyn/recenzje/moulthrop.htm'
                                   ]},
                '2': {'numer': 'https://techsty.art.pl/magazyn/magazyn2.html',
                      'rok': 2006,
                      'artykuły': ['https://www.techsty.art.pl/magazyn2/artykuly/aarseth_cybertekst.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/eskelinen_cybertekst.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/stunza_webeo.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/moulthrop_wywiad.html'
                                   ]},
                '3': {'numer': 'https://techsty.art.pl/magazyn/magazyn3.html',
                      'rok': 2007,
                      'artykuły': ['https://techsty.art.pl/magazyn3/bluzgator_bis.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/pisarski01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/branny01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/pajak01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/bogaczyk01.html',
                                   'https://techsty.art.pl/magazyn3/recenzje/sikora01.html',
                                   'https://techsty.art.pl/magazyn3/amerika_wirusy.html'
                                   ]},
                '5': {'numer': 'https://techsty.art.pl/magazyn/magazyn5.html',
                      'rok': 2008,
                      'artykuły': ['https://techsty.art.pl/magazyn/magazyn5/recenzje/koniec_ksiazki.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/galaktyka_jezyka_internetu.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/net.art.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/shadows.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/mnemotechniki.html'
                                   ]},
                '6': {'numer': 'https://techsty.art.pl/magazyn/magazyn6.html',
                      'rok': 2009,
                      'artykuły': ['https://techsty.art.pl/magazyn/magazyn6/artykuly/pisarski01.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/artykuly/lee01.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/artykuly/sikora01.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/e-poetry01.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/rec/playing_with_videogames.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/rec/dlugi_ogon.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/rec/cyfrowa_rewolucja.html',
                                   'https://techsty.art.pl/magazyn/magazyn6/rec/kultura_szerokopasmowa.html'
                                   ]},
                '7': {'numer': 'https://techsty.art.pl/magazyn/magazyn7.html',
                      'rok': 2011,
                      'artykuły': ['https://techsty.art.pl/magazyn/magazyn7/literatura_elektroniczna_czym_jest_1.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/artykuly/gryglicka01.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/cybertext_yearbook_2010.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/rec/marsz_jasienskiego.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/rec/Sufferrosa.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/rec/original_of_laura.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/rec/noce_i_petle.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/wywiad_z_dawidem_marcinkowskim.html',
                                   'https://techsty.art.pl/magazyn/magazyn7/wywiad_z_leszkiem_onakiem.html'
                                   ]},
                '8': {'numer': 'https://techsty.art.pl/magazyn/magazyn8.html',
                      'rok': 2012,
                      'artykuły': ['https://techsty.art.pl/magazyn/ludologiczny/johnson01.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/zapomniana_klasyka.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/sonia_fizek01.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/falkowska01.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/pisarski_01.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/ankiety_wstep.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/questionaire_en.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/ankiety_PL.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/o_autorach.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/puszka_pandory_en.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/smok_wawelski_en.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/mozgprocesor_en.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/tekstowe_gry_niezalezne.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/rec_gibb.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/rec_joyce.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/rec/cyfrowa_doroslosc.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/rec/wokol_mediow_20.html',
                                   'https://techsty.art.pl/magazyn/ludologiczny/rec/media_audiowizualne.html'
                                   ]}}
    
#%% main
all_results = []

for issue, d in tqdm(techsty_dict.items()):
    # issue = '1'
    # d = techsty_dict.get(issue)
    for link in d.get('artykuły'):
        # art = 'https://techsty.art.pl/magazyn/coover/coover.htm'
        # art = 'https://techsty.art.pl/magazyn/mochola/pol1.htm'
        html_text = requests.get(link)
        html_text.encoding = 'utf-8'
        html_text = html_text.text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = f"{d.get('rok')}-01-01"
       
        content_of_article = soup.find('div', class_='col-md-9 magazyn-artykul')
        
        if not content_of_article:
            content_of_article = soup.find('div', class_='col-sm-9 magazyn-artykul')
        
        if not content_of_article:
            content_of_article = soup.find('div', {'id': 'maintext'})
            
        # tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')][1:])
        
        author = soup.find('h4')
        if not author:
            author = soup.find('h3').text
        else: author = author.text
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article]).strip()
        
        title_of_article = soup.find('h2')
     
        if not title_of_article:
            try:
                title_of_article = soup.find_all('h1')[1].text
            except IndexError:
                title_of_article = soup.find('h4').text
        else: title_of_article = title_of_article.text
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'techsty' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in content_of_article.find_all('img')])
        except KeyError:
            photos_links = None
    
        dictionary_of_article = {'Link': link,
                                 'Data publikacji': date_of_publication,
                                 'Autor': author,
                                 'Tytuł artykułu': title_of_article,
                                 'Tekst artykułu': text_of_article,
                                 'Linki zewnętrzne': external_links,
                                 'Linki do zdjęć': photos_links
                                 }
    
        all_results.append(dictionary_of_article)
    

#%%

with open(f'data/techsty_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/techsty_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/techsty_{datetime.today().date()}.xlsx", f'data/techsty_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















