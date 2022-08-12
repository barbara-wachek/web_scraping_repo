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




def biala_fabryka_web_scraping_sitemap(link):
    sitemap = 'https://bialafabryka.blogspot.com/sitemap.xml'
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
        
        article_link = 'https://bialafabryka.blogspot.com/2022/08/pare-spraw-domknac.html'
        #article_link = 'https://bialafabryka.blogspot.com/2009/01/crisis-what-crisis.html'
        #article_link = 'https://bialafabryka.blogspot.com/2010/05/z-krwi-i-kosci.html'
        #article_link = 'https://bialafabryka.blogspot.com/2021/11/wypasione-chaty-rzedem.html'
        #article_link = 'https://bialafabryka.blogspot.com/2015/10/piekna-nie-opuszczaj-jej-nie-opuszczaj.html'
        #article_link = 'https://bialafabryka.blogspot.com/2009/01/zdjecia-kwarca.html'
       # article_link = 'https://bialafabryka.blogspot.com/2009/12/znaki-zapytania.html'
        #article_link = 'https://bialafabryka.blogspot.com/2021/11/wypasione-chaty-rzedem.html'
        
        
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
            tags_span = soup.find_all('span', class_='post-labels')
            tags = [tag.text for tag in tags_span][0].strip().split('\n')
      
        except AttributeError:
            pass 
        except IndexError:   
            pass
    
    
        for element in texts_of_article:
            try:
                article = element.text.strip().replace('\n', ' ')
               
                dictionary_of_article['Link'] = article_link
                dictionary_of_article['Data publikacji'] = new_date
                
            
                title_of_article = soup.find('h3', class_='post-title entry-title').text.strip()
                dictionary_of_article['Tytuł artykułu'] = title_of_article
                
                
                dictionary_of_article['Tekst artykułu'] = article
                
                if re.findall(r'(Autor\:)\s(Piotr Gajda)', article):
                    dictionary_of_article['Autor'] = 'Piotr Gajda'
                elif re.findall(r'\(pg\)', article):
                    dictionary_of_article['Autor'] = 'Piotr Gajda'
                else: 
                    dictionary_of_article['Autor'] = 'Krzysztof Kleszcz'

                
                dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
                
                if element.find_all('h2'):
                    dictionary_of_article['Opis książki'] = [x.text for x in element.find_all('h2')][0].strip().replace('\n', ' ')
                
                
                
                
            except AttributeError:
                pass 
            except IndexError:   
                pass
            
        
            try:
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
    df.to_excel(f"bialafabryka_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
           
    
    
    
    
    
    
    
    
