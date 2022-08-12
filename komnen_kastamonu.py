import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor


from datetime import datetime
from time import mktime


def komnem_kastamonu_web_scraping_sitemap(link):
    sitemap = 'https://komnen-kastamonu.blogspot.com/sitemap.xml'
    html_text_sitemap = requests.get(sitemap).text
    soup = BeautifulSoup(html_text_sitemap, 'lxml')
    links = [e.text for e in soup.find_all('loc')]
    
    articles_links = []
    
    
    def get_article_pages(link):   
       
        html_text_sitemap = requests.get(link).text
        soup = BeautifulSoup(html_text_sitemap, 'lxml')
        sitemap_links = [e.text for e in soup.find_all('loc')]
        articles_links.extend(sitemap_links)
       
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(get_article_pages, links),total=len(links)))
        
    def dictionary_of_article(article_link):
        
       #article_link = 'http://komnen-kastamonu.blogspot.com/2021/12/zadrutowannosc.html'
       #article_link = 'http://komnen-kastamonu.blogspot.com/2019/12/corocznik-wigilijnik.html'
       #article_link = 'http://komnen-kastamonu.blogspot.com/2019/12/kazdy-swir-ma-swoj-kraj.html'
       #article_link = 'http://komnen-kastamonu.blogspot.com/2019/11/nowe-wiersze-cakiem-swieze-black-friday.html'
       #article_link = 'http://komnen-kastamonu.blogspot.com/2019/06/nikogo-nie-obchodzi-o-czym-opowiadasz.html'
       #article_link = 'http://komnen-kastamonu.blogspot.com/2019/06/kiedy-grabaz-zna-moje-mysli.html'
       #article_link = 'https://komnen-kastamonu.blogspot.com/2015/01/183-rocznica-urodzin-lewisa-carolla-3.html'
       #article_link = 'https://komnen-kastamonu.blogspot.com/2012/01/tell-me-are-you-locked-in-punch.html'
       #article_link = 'https://komnen-kastamonu.blogspot.com/2011/12/i-zasypiaja-z-morda-na-piersi-w.html'
       
       html_text = requests.get(article_link).text
       while 'Error 503' in html_text:
           time.sleep(2)
           html_text = requests.get(article_link).text
       soup = BeautifulSoup(html_text, 'lxml')
       
       dictionary_of_article = {}
       
       #DATA
       
       date_of_publication = soup.find('h2', class_='date-header').text
       date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication)
  
       lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
       s = date
       for k, v in lookup_table.items():
           s = s.replace(k, v)
       
       result = time.strptime(s, "%d %m %Y")
       changed_date = datetime.fromtimestamp(mktime(result))   
       new_date = format(changed_date.date())
       
       
       try:
           texts_of_article = soup.find_all('div', class_='post-body entry-content')
          
       except AttributeError:
           pass 
       except IndexError:   
           pass
        
        
        
       
       #Autor nie stosuje tagów!
       
       for element in texts_of_article:
            try:
                article = element.text.strip().replace('\n', ' ')
               
                dictionary_of_article['Link'] = article_link
                dictionary_of_article['Data publikacji'] = new_date
                dictionary_of_article['Autor'] = 'Paweł Podlipniak'
                
                title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
                dictionary_of_article['Tytuł artykułu'] = title_of_article
                
                dictionary_of_article['Tekst artykułu'] = article.replace('\xa0', '')

            
            except AttributeError:
                pass 
            except IndexError:   
                pass
            
        
            try:
    
                
                list_of_poems = [x.text for x in element.find_all('b')]  
                titles_of_poems = [x.strip() for x in list_of_poems if x != '\xa0' and x != '' and x != '\n']
                
                dictionary_of_article['Tytuły wierszy'] = " | ".join(titles_of_poems).replace('\n', ' ')
                
                links_in_article = [x['href'] for x in element.find_all('a')]
                dictionary_of_article['Linki_zewnętrzne'] = ' | '.join([x for x in links_in_article if not re.findall(r'blogspot|jpg', x)])
                
                
                
                list_of_images = [x['src'] for x in element.find_all('img')]
                if list_of_images != []:
                    dictionary_of_article['Zdjęcia/Grafika'] = 'TAK'
                    
                
                list_of_video = [x['src'] for x in element.find_all('iframe')]
                if list_of_video != []:
                    dictionary_of_article['Filmy'] = 'TAK'
                    
                
            except AttributeError:
                pass 
            except IndexError:   
                pass   
            
            

            all_results.append(dictionary_of_article)
    
    
    all_results = []
    
    with ThreadPoolExecutor() as excecutor:
        list(tqdm(excecutor.map(dictionary_of_article, articles_links),total=len(articles_links)))
        
        
    df = pd.DataFrame(all_results)
    df.to_excel(f"komnen_kastamonu_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
        
       
            