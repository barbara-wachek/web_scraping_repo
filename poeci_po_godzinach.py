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




def poeci_po_godzinach_web_scraping_sitemap(link):
    sitemap = 'https://poecipogodzinach.blogspot.com/sitemap.xml'
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
        
        #article_link = 'http://poecipogodzinach.blogspot.com/2020/12/potrafie-czuwac.html'
        #article_link = 'http://poecipogodzinach.blogspot.com/2016/05/ktos-chcia-widziec-we-mnie-ciagle-nowe.html'
        
        #article_link = 'http://poecipogodzinach.blogspot.com/2018/06/smutna-jest-moja-dusza-az-do-smierci.html'
        #article_link = 'http://poecipogodzinach.blogspot.com/2018/12/wszystko-juz-byo-w-srodku-na-zewnatrz.html'
        
        #article_link = 'https://poecipogodzinach.blogspot.com/2015/04/blog-post.html'
        #article_link = 'https://poecipogodzinach.blogspot.com/2016/03/do-widzenia-do-jutra-jakobe-mansztajn-i.html'
        #article_link = 'http://poecipogodzinach.blogspot.com/2014/06/karol-maliszewski-i-krzysztof-kleszcz.html' #z filmami
        #article_link = 'https://poecipogodzinach.blogspot.com/2015/02/czternascie.html'  #kilku autorów w jednym poscie
        
        
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
                dictionary_of_article['Tagi'] = '| '.join(tags[1:]).replace(',', ' ')
                
                
                dictionary_of_article['Autor wiersza'] = re.findall(r'^\p{Lu}{1}[\p{Ll}.]*\s\p{Lu}{1}\p{Ll}*\-?\p{Lu}*\p{Ll}*', article)[0]
                
                list_of_poems = [x.text for x in element.find_all('b')]  
                titles_of_poems = [x.strip() for x in list_of_poems if x != '\xa0']
                
                
                dictionary_of_article['Tytuły wierszy'] = " | ".join(titles_of_poems).replace('\n', ' ')
                
            
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
    df.to_excel(f"poeci_po_godzinach_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   
       
        
   
    
   
    #Kod ignoruje linki, jęsli jest ich za duzo (nie mieszczą się w excelu) chyba trzeba będzie z tego zrezygnowac? albo zalozyc ze mamy niepelne dane
    #albo po prostu odsyłać w tabeli do wersji z webarchive? 
   
    #CZESC DO OPRACOWANIA: (pobieranie wszystkich grafik z bloga do jednego folderu, w nazwie pliku ma być mozliwoć zlokalizowania na stronie) 
    
    
    
    sitemap = 'https://poecipogodzinach.blogspot.com/sitemap.xml'
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
    
    
    
    def image_download(article_link): 
           
        article_link = 'http://poecipogodzinach.blogspot.com/2018/12/wszystko-juz-byo-w-srodku-na-zewnatrz.html'
        
        html_text = requests.get(article_link).text
        while 'Error 503' in html_text:
            time.sleep(2)
        html_text = requests.get(article_link).text
        soup = BeautifulSoup(html_text, 'lxml')
        
        try:
            images = [x['src'] for x in soup.find_all('img')]
            name_of_website = re.sub(r'(https\:\/\/)(.*)(\/.*)', r'\2', sitemap)
            number = 0
            for image in images:
                number =+ 1
                with open(name_of_website + "_" + new_date + '_' + str(number) + '.jpg', 'wb') as f:
                    im = requests.get(image)
                    f.write(im.content)
                    
                    
        except AttributeError:
            pass 
        except IndexError:   
            pass
            
    with ThreadPoolExecutor() as excecutor:
       list(tqdm(excecutor.map(image_download, articles_links), total=len(articles_links)))  
        
        
        
 #problem: gdy jest za dużo linkóW do zdjęc w jednym artykule excel nie moze ich pomiecić i zawartosc danej komorki w tabeli jest pusta       
 #Trzeba podjac decyzje w jaki sposob archiwzujemy grafiki - niektore sa bardzowazne np. przyklad grafiki na ktorej jest skan wiersza (nie ma go w artykule w wersji tekstowej)
        
   
        
          
    
    
    
    
        
        
            
            
            
            
       
        #folder = re.sub(r'(https\:\/\/)(.*)(\/.*)', r'\2', sitemap) + '_images'
        #os.mkdir(os.path.join(os.getcwd(), folder))
        #os.chdir(os.path.join(os.getcwd(), folder))                 
       
               
               #number = 0
               #for link in list_of_images:
                   #number =+ 1
                   #with open(name_of_image_file + "_" + new_date + '_' + str(number) + '.jpg', 'wb') as f:
                       #image = requests.get(link)
                       #f.write(image.content)   
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        