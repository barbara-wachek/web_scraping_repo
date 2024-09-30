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
                      'artykuły': ['https://techsty.art.pl/magazyn/coover/coover.htm',
                                   'https://www.techsty.art.pl/magazyn/bernstein/b01.htm',
                                   'https://techsty.art.pl/magazyn/mochola/pol1.htm',
                                   'https://techsty.art.pl/magazyn/pisarski/p01.htm',
                                   'https://techsty.art.pl/magazyn/sikora/s01.htm',
                                   'https://techsty.art.pl/magazyn/stunza/01.htm',
                                   'https://techsty.art.pl/magazyn/net-art-sztuka-wobec-interaktywnosci.htm',
                                   'https://techsty.art.pl/magazyn/mochola/mph01.htm',
                                   'https://techsty.art.pl/magazyn/m1.htm',
                                   'https://techsty.art.pl/magazyn/r1.htm',
                                   'https://techsty.art.pl/magazyn/recenzje/drmuto.htm',
                                   'https://techsty.art.pl/magazyn/sikora/bloody.htm',
                                   'https://techsty.art.pl/magazyn/recenzje/moulthrop.htm'
                                   ]},
                '2': {'numer': 'https://techsty.art.pl/magazyn/magazyn2.html',
                      'artykuły': ['https://www.techsty.art.pl/magazyn2/artykuly/aarseth_cybertekst.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/eskelinen_cybertekst.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/stunza_webeo.html',
                                   'https://techsty.art.pl/magazyn2/artykuly/moulthrop_wywiad.html'
                                   ]},
                '3': {'numer': 'https://techsty.art.pl/magazyn/magazyn3.html',
                      'artykuły': ['https://techsty.art.pl/magazyn3/bluzgator_bis.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/pisarski01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/branny01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/pajak01.html',
                                   'https://techsty.art.pl/magazyn3/artykuly/bogaczyk01.html',
                                   'https://techsty.art.pl/magazyn3/recenzje/sikora01.html',
                                   'https://techsty.art.pl/magazyn3/amerika_wirusy.html'
                                   ]},
                '5': {'numer': 'https://techsty.art.pl/magazyn/magazyn5.html',
                      'artykuły': ['https://techsty.art.pl/magazyn/magazyn5/recenzje/koniec_ksiazki.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/galaktyka_jezyka_internetu.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/net.art.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/shadows.html',
                                   'https://techsty.art.pl/magazyn/magazyn5/recenzje/mnemotechniki.html'
                                   ]},
                '6': {'numer': 'https://techsty.art.pl/magazyn/magazyn6.html',
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

#%% def
#sitemap might be broken https://www.aict.art.pl/post-sitemap.xml
def get_links_of_sitemap(sitemap_link):
    # sitemap_link = 'https://www.aict.art.pl/post-sitemap.xml'
    html_text = requests.get(sitemap_link).text
    soup = BeautifulSoup(html_text, "html.parser")
    links = [x.text for x in soup.find_all('loc') if re.findall(r'www.aict.art.pl/\d+', x.text)]
    return links

def dictionary_of_article(link):
    # link = 'https://www.aict.art.pl/2024/02/24/prapremiera-geniusza-w-rezyserii-jerzego-stuhra-w-teatrze-polonia/'
    try:
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = soup.find('time')['datetime'][:10]
       
        content_of_article = soup.find('div', class_='nv-content-wrap entry-content')
        
        # tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')][1:])
        
        author = [e.find('strong').text for e in content_of_article.find_all('p') if e.find('strong')]
        try:
            author = [e for e in author if e[0].isupper() and e[1].islower() and e.split(' ')[1][0].isupper()]
            if author:
                author = author[0]
            else:
                author = [e.find('b').text for e in content_of_article.find_all('p') if e.find('b')]
                try:
                    author = [e for e in author if e[0].isupper() and e[1].islower()]
                    if author:
                        author = author[0]
                    else:
                        author = None
                except:
                    author = None
        except IndexError:
            author = ''.join([el.text for sub in [e.find_all('strong') for e in content_of_article.find_all('p') if e.find('strong')] for el in sub])
            if len(author) > 100:
                author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article])
        
        title_of_article = soup.find('h1', class_='title entry-title')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'aict.art.pl' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])
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
    except TypeError:
        errors.append(link)
    
#%% main

for issue, d in tqdm(techsty_dict.items()):
    issue = '1'
    d = techsty_dict.get(issue)
    for art in d.get('artykuły'):
        art = d.get('artykuły')[0]
        html_text = requests.get(art).text
        soup = BeautifulSoup(html_text, 'html.parser')
        
        date_of_publication = soup.find('ol')
        soup.find('time')['datetime'][:10]
       
        content_of_article = soup.find('div', class_='nv-content-wrap entry-content')
        
        # tags = '|'.join([e.text for e in soup.find_all('a', rel='tag')][1:])
        
        author = [e.find('strong').text for e in content_of_article.find_all('p') if e.find('strong')]
        try:
            author = [e for e in author if e[0].isupper() and e[1].islower() and e.split(' ')[1][0].isupper()]
            if author:
                author = author[0]
            else:
                author = [e.find('b').text for e in content_of_article.find_all('p') if e.find('b')]
                try:
                    author = [e for e in author if e[0].isupper() and e[1].islower()]
                    if author:
                        author = author[0]
                    else:
                        author = None
                except:
                    author = None
        except IndexError:
            author = ''.join([el.text for sub in [e.find_all('strong') for e in content_of_article.find_all('p') if e.find('strong')] for el in sub])
            if len(author) > 100:
                author = None
        
        text_of_article = '\n'.join([e.text.strip().replace('\n', '') for e in content_of_article])
        
        title_of_article = soup.find('h1', class_='title entry-title')
     
        if title_of_article:
            title_of_article = title_of_article.text.strip()     
        else:
            title_of_article = None
    
        try:
            external_links = ' | '.join(set([el['href'] for sub in [e.find_all('a') for e in content_of_article.find_all('p')] for el in sub if 'aict.art.pl' not in el['href']]))
        except KeyError:
            external_links = None
            
        try:
            photos_links = ' | '.join([x['src'] for x in soup.find('article').find_all('img')])
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
    except TypeError:
        errors.append(link)
    



articles_links = get_links_of_sitemap('https://www.aict.art.pl/post-sitemap.xml')

all_results = []
errors = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))   

with open(f'data/aict_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f, ensure_ascii=False, default=str)        

df = pd.DataFrame(all_results)
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with pd.ExcelWriter(f"data/aict_{datetime.today().date()}.xlsx", engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:    
    df.to_excel(writer, 'Posts', index=False)

#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f"data/aict_{datetime.today().date()}.xlsx", f'data/aict_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'title': upload_file.replace('data/', ''), 'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})
	gfile.SetContentFile(upload_file)
	gfile.Upload()  

















