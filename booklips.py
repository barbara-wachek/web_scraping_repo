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
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from functions import date_change_format_short

#%% def
def get_links_of_sitemap_links_posts(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap-pt-)(page)(-\d{4}-\d{2}\.xml)', x.text) and not re.findall(r'https\:\/\/booklips\.pl\/sitemap-misc\.xml', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_posts.extend(articles_links)
    
    return all_articles_links_posts
 

def dictionary_of_article(link):
    #link = 'https://booklips.pl/newsy/zmarl-kevin-oneill-wspoltworca-serii-komiksowej-liga-niezwyklych-dzentelmenow/'
    #link = 'http://booklips.pl/czytelnia/opowiadania/nieznana-historia-milosna-pajaka-jednego-z-bohaterow-serii-kolory-zla-przeczytaj-opowiadanie-kryminalne-cytrusy-i-migdaly-malgorzaty-oliwii-sobczak/'
    #link = 'https://booklips.pl/recenzje/w-pulapce-bez-wyjscia-recenzja-ksiazki-ruiny-scotta-smitha/' #recenzja
    #link = 'https://booklips.pl/recenzje/suplement-do-masakry-recenzja-komiksu-rzeznia-numer-piec-ryana-northa-i-alberta-monteysa/' #recenzja
    #link = 'https://booklips.pl/recenzje/powrot-krola-recenzja-ksiazki-basniowa-opowiesc-stephena-kinga/' #rec
    #link = 'https://booklips.pl/recenzje/w-trybach-rewolucji-recenzja-komiksu-wolnosc-albo-smierc-aleksandry-herzyk/' #rec.
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/dluga-podroz-pewnej-opowiesci-przeczytaj-fragment-miasta-w-chmurach-anthonyego-doerra/' #czyt.
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/brutalnie-szczera-opowiesc-o-relacji-miedzy-umierajaca-matka-a-dorosla-corka-przeczytaj-fragment-ksiazki-ostatni-raz-helgi-flatland/'
    # link = 'https://booklips.pl/wywiady/przerwac-milczenie-opowiescia-rozmowa-z-carolina-de-robertis-autorka-cantoras/' #wywiad
    
    # link = 'https://booklips.pl/adaptacje/film/amazon-studios-prezentuje-drugi-zwiastun-serialu-wladca-pierscieni-pierscienie-wladzy/' #adaptacja filmowa
    #link = 'https://booklips.pl/recenzje/wykrecone-na-druga-strone-recenzja-komiksu-zasada-trojek-tomasza-spella/' # rec.
    #link = 'https://booklips.pl/recenzje/historia-o-ogromnym-potencjale-recenzja-ksiazki-czarne-skrzydla-czasu-diane-setterfield/'
    # link = 'https://booklips.pl/adaptacje/film/zwiastun-filmu-oficer-i-szpieg-romana-polanskiego-nakreconego-na-podstawie-powiesci-roberta-harrisa/'
    # link = 'https://booklips.pl/artykuly/kaznodzieja-festiwal-przemocy-i-brutalnosci/'
    #link = 'https://booklips.pl/artykuly/wspolczesny-bajarz-terry-pratchett/'
    # link = 'https://booklips.pl/biurka-polskich-pisarzy/filip-zawada/'
    # link = 'https://booklips.pl/ciekawostki/dlaczego-henryk-sienkiewicz-otrzymal-nobla-i-jak-do-tego-doszlo-ze-nie-podzielil-sie-nagroda-z-eliza-orzeszkowa/'
    # link = 'https://booklips.pl/ciekawostki/dlaczego-a-j-finn-publikuje-pod-pseudonimem-wyjasniamy-zagadke-autora-kobiety-w-oknie/'
    # link = 'https://booklips.pl/ciekawostki/fantastyczny-wywiad-z-michelem-houellebekiem/'
    # link = 'https://booklips.pl/ciekawostki/ostatni-wiersz-charlesa-bukowskiego-przeslany-faksem/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/gra-w-pilke-ludzka-czaszka-przeczytaj-fragment-powiesci-bog-tak-chcial-arka-gieszczyka/'
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/akcja-wisla-motywem-przewodnim-nowej-powiesci-roberta-nowakowskiego-przeczytaj-przed-premiera-fragment-ojczyzny-jablek/'
    #link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/o-dwoch-kobietach-fragment-uhonorowanej-nagroda-literacka-unii-europejskiej-powiesci-wyspa-krach-iny-wylczanowej/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/kultowa-zazi-w-metrze-raymonda-queneau-dostepna-w-ksiegarniach-przeczytaj-poczatek-ksiazki/'
    #link = 'https://booklips.pl/galeria/eros-i-tanatos-na-ilustracjach-z-1934-roku-do-kwiatow-zla-charlesa-baudelairea/'
    #link = 'https://booklips.pl/newsy/zlodziej-manuskryptow-zatrzymany-przez-fbi-od-ponad-pieciu-lat-podszywal-sie-pod-przedstawicieli-branzy-literackiej-by-zyskac-dostep-do-ksiazek-przed-premiera/'
    # link = 'https://booklips.pl/newsy/w-nowym-albumie-lucky-luke-bedzie-walczyl-z-rasizmem-na-glebokim-poludniu/'
    # link = 'https://booklips.pl/adaptacje/film/zakonczono-zdjecia-do-filmu-na-podstawie-ksiazki-teczowe-san-francisco-alysii-abbott-za-produkcje-odpowiada-sofia-coppola/'
    # link = 'https://booklips.pl/adaptacje/film/superbohaterowie-filmowa-historia-o-zwiazkach-i-roli-jaka-odgrywa-w-nich-czas-ktora-paolo-genovese-nakrecil-na-podstawie-wlasnej-powiesci/'
    #link = 'https://booklips.pl/adaptacje/film/marcin-dorocinski-w-roli-edwarda-popielskiego-premiera-serialu-erynie-borysa-lankosza-juz-25-pazdziernika/'
    #link = 'https://booklips.pl/biurka-polskich-pisarzy/jaroslaw-maslanek/'
    # link = 'https://booklips.pl/adaptacje/film/pierwszy-zwiastun-filmu-ziarno-prawdy-na-podstawie-powiesci-zygmunta-miloszewskiego/'
    # link = 'https://booklips.pl/ciekawostki/albert-einstein-wyjasnia-dlaczego-warto-czytac-klasykow/'
    # link = 'https://booklips.pl/ciekawostki/najlepszy-dowod-uznania-jaki-maurice-sendak-otrzymal-od-malego-czytelnika/'
    # link = 'https://booklips.pl/newsy/marianna-kijanowska-dziekuje-za-nagrode-herberta-2022-na-gale-nie-mogla-przyjechac-z-powodow-zdrowotnych/'
    # link = 'https://booklips.pl/czytelnia/listy/thomas-wolfe-opisuje-pijackie-przezycie-z-oktoberfestu-w-1928-roku/'
    # link = 'https://booklips.pl/premiery-i-zapowiedzi/wielki-upadek-petera-handkego-gesta-znaczeniowo-opowiesc-o-kondycji-wspolczesnego-czlowieka/'
    # link = 'https://booklips.pl/recenzje/mroczna-basniowosc/'
    #link = 'https://booklips.pl/recenzje/koniec-swiata-to-tylko-poczatek/'
    #link = 'https://booklips.pl/czytelnia/opowiadania/czy-roxane-gay-wywola-podobne-kontrowersje-jak-smarzowski-przeczytaj-opowiadanie-zly-ksiadz-ze-zbioru-histeryczki/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/bronislaw-pilsudski-wsrod-ajnow-fragment-powiesci-akan-pawla-gozlinskiego/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/przeczytaj-fragment-powiesci-a-hipopotamy-zywcem-sie-ugotowaly-williama-s-burroughsa-i-jacka-kerouaca/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/fragment-powiesci-potomstwo-jacka-ketchuma/'
    # link = 'https://booklips.pl/czytelnia/opowiadania/maciej-gierszewski-pani-kocikowa/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/fragment-powiesci-wampir-z-mo-andrzeja-pilipiuk/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/kultowa-zazi-w-metrze-raymonda-queneau-dostepna-w-ksiegarniach-przeczytaj-poczatek-ksiazki/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/fragment-powiesci-zywe-trupy-droga-do-woodbury-kirkmana-i-bonansingi/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/fragment-powiesci-kobieta-w-1000o-c-hallgrimura-helgasona/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/przeczytaj-fragment-biografii-grace-ksiezna-monako-jeffreya-robinsona/'
    # link = 'https://booklips.pl/czytelnia/fragmenty-ksiazek/karol-i-max-rozmawiaja-o-sprawach-ostatecznych-fragment-powiesci-glacier-express-9-15-janusza-majewskiego/'
    # link = 'https://booklips.pl/przeglad/radio/85-rocznica-urodzin-marka-nowakowskiego-radiowa-dwojka-zaprasza-na-serie-audycji-i-sluchowisk-poswieconych-pisarzowi/'
    # link = 'https://booklips.pl/recenzje/sledztwo-ktore-ciagnie-sie-jak-makaron/'
    # link = 'https://booklips.pl/recenzje/kafka-do-potegi-kusturicy/'
    # link = 'https://booklips.pl/recenzje/raport-z-oblezonego-miasta-recenzja-komiksu-dmz-strefa-zdemilitaryzowana-tom-1-briana-wooda-i-riccarda-burchielliego/'
    # #link = 'https://booklips.pl/recenzje/show-jima-czyli-historia-taty-muppetow/'
    # #link = 'https://booklips.pl/recenzje/wejsc-w-umysl-agenta-chaosu/'
    # link = 'https://booklips.pl/recenzje/stary-dobry-swiat-nie-byl-nigdy-dobry/'
    # link = 'https://booklips.pl/recenzje/metropolia-z-piekielnej-otchlani/'
    #link = 'https://booklips.pl/recenzje/jasniejaca-prawda-wydobyta-z-mrokow-prowincji/'
 
    html_text = requests.get(link).content
    soup = BeautifulSoup(html_text, 'lxml')
    
    date_of_publication = soup.find('span', class_='meta-date')
    if date_of_publication: 
        date_of_publication = date_of_publication.text 
        new_date = date_change_format_short(date_of_publication)
    else:
        new_date = None
        
    title_of_article = soup.find('h1', class_='post-title single entry-title')
    if title_of_article:
        title_of_article = title_of_article.text
    else:
        title_of_article = None
        
        
    category = re.sub(r'(https?\:\/\/booklips\.pl\/)(\w*\-?\w*\-?\w*)(\/.*)', r'\2', link)
    content_of_article = soup.find('div', class_='entry')
    
    text_of_article = [x.text.replace('\n', ' ') for x in content_of_article.find_all('p', class_=None)]
    if text_of_article:
        text_of_article = " ".join(text_of_article).strip()
    else:
        text_of_article = None

    tags = content_of_article.find('p', class_='tags')
    if tags: 
        tags = ' | '.join([x.text for x in content_of_article.find('p', class_='tags').findChildren('a')])
    else:
        tags = None
          

    external_links = [x for x in [x['href'] for x in content_of_article.find_all('a')] if not re.findall(r'(booklips)|(http://twitter.com/share)', x)]
    if external_links != []:
        external_links = ' | '.join(external_links)
    else:
        external_links = None
        

    photos_links = [x['src'] for x in content_of_article.find_all('img')]
    if photos_links != []:
        photos_links = ' | '.join(photos_links)
    else:
        photos_links = None
             
    
    author = None 
    for key,value in dictionary_of_authors.items():
        if key in text_of_article: 
            author = key
        elif value in text_of_article: 
            author = value



    title_of_adaptation = None
    if category == 'adaptacje':
        if re.search(r'\„.*”', title_of_article):
            title_of_adaptation = re.search(r'\„.*”', title_of_article).group(0)
        else: 
            title_of_adaptation = 'DO UZUPEŁNIENIA'
    
    
    if re.search(r'(https:\/\/booklips\.pl\/adaptacje\/)(film|muzyka|sluchowiska|teatr|gry)(\/.*)', link):
        type_of_adaptation = re.sub(r'(https:\/\/booklips\.pl\/adaptacje\/)(film|muzyka|sluchowiska|teatr|gry)(\/.*)', r'\2', link)
    else:
        type_of_adaptation = None


    title_of_book = None
    author_of_book = None
    if category == 'recenzje':
        if re.search(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', text_of_article):
            title_and_author_of_book = re.search(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', text_of_article).group(0)
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-\:\,\’]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)(Ocena)', r'\1', title_and_author_of_book).strip()
            
        else:
            title_of_book = 'DO UZUPEŁNIENIA'
            author_of_book = 'DO UZUPEŁNIENIA'
       
    
    if category == 'premiery-i-zapowiedzi':
        if re.search(r'„.*”', title_of_article):
            title_of_book = re.search(r'„.*”', title_of_article).group(0)

    
    #if re.search(r'fragmenty-ksiazek', link):
    if category == 'czytelnia':
        title_and_author_of_book = [x.text for x in content_of_article.find_all('strong') if re.findall(r'„[\p{L}\s\'\.\?\d\–\-]*”', x.text)]
        if len(title_and_author_of_book) >= 2:
            title_and_author_of_book = "".join(title_and_author_of_book[-1].strip())
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\1', title_and_author_of_book).strip()
        elif len(title_and_author_of_book) == 1:
            title_and_author_of_book = "".join(title_and_author_of_book)
            title_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\2', title_and_author_of_book).strip()
            author_of_book = re.sub(r'^(.*)(„[\p{L}\s\'\.\?\d\–\-]*”)(\,?\s?)(tłum\.|wyd\.)?(.*)', r'\1', title_and_author_of_book).strip()
        else:
            title_of_book = 'DO UZUPEŁNIENIA'
            author_of_book = 'DO UZUPEŁNIENIA'
   
    if re.search(r'czytelnia\/(listy|przedruki|opowiadania|wiersze|fragmenty-ksiazek)', link):   
        contributor = author
        author = None
    else:
        contributor = None    
        
        
    rating = None        
    if re.search(r'(Ocena\:?\s\d\,?\d?\s?\/\s\d{2})', text_of_article):
        rating = re.search(r'(Ocena\:?\s\d\,?\d?\s?\/\s\d{2})', text_of_article).group(0).replace('Ocena', '').replace(':',"").strip()
    else:
        rating = None
    
    
    related = None       
    if re.search(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', title_of_article):
        related = re.sub(r'(Biurka\spolskich\s)(pisarzy|komiksiarzy)(\:\s)(.*)', r'\4', title_of_article)
    elif re.search(r'(https:\/\/booklips\.pl\/przeglad\/)([\w\-]*)(\/.*)', link):
        related = re.sub(r'(https:\/\/booklips\.pl\/przeglad\/)([\w\-]*)(\/.*)', r'\2', link)
    else:
        related = None
           
    
    dictionary_of_article = {'Link': link, 
                             'Data publikacji': new_date, 
                             'Autor': author,
                             'Współtwórca': contributor,
                             'Kategoria': category,
                             'Tytuł artykułu': title_of_article,
                             'Tekst artykułu': text_of_article,
                             'Tagi': tags,
                             'Autor książki/dzieła': author_of_book,
                             'Tytuł książki/dzieła': title_of_book, 
                             'Typ adaptacji': type_of_adaptation,
                             'Tytuł adaptacji': title_of_adaptation,
                             'Wpis dotyczy': related,
                             'Ocena książki': rating,
                             'Linki zewnętrzne': external_links,
                             'Zdjęcia/Grafika': True if [x['src'] for x in content_of_article.find_all('img')] else False,
                             'Filmy': True if [x['src'] for x in content_of_article.find_all('iframe')] else False,
                             'Linki do zdjęć': photos_links
                             }

    all_results_posts.append(dictionary_of_article)
    
    
def get_links_of_sitemap_links_pages(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    links = [x.text for x in soup.find_all('loc') if not re.findall(r'(https\:\/\/booklips\.pl\/sitemap-pt-)(post)(-\d{4}-\d{2}\.xml)', x.text) and not re.findall(r'https\:\/\/booklips\.pl\/sitemap-misc\.xml', x.text)]
    
    for link in tqdm(links):
    
        html_text = requests.get(link).text
        soup = BeautifulSoup(html_text, 'lxml')
        articles_links = [x.text for x in soup.find_all('loc')]
        all_articles_links_pages.extend(articles_links)
    
    return all_articles_links_pages    
    
#%% main
sitemap_links = ['https://booklips.pl/sitemap.xml']   

dictionary_of_authors = {'[am]': 'Artur Maszota', 
                         '[kch]': 'Karolina Chymkowska',
                         '[mw]': 'Mariusz Wojteczek',
                         '[edm]': 'Emilia Dulczewska-Maszota',
                         '[aw]': 'Anna Wyrwik', 
                         '[kch,am]': 'Karolina Chymkowska | Artur Maszota',
                         '[am,kch]': 'Artur Maszota | Karolina Chymkowska',
                         '[am,mw]': 'Artur Maszota | Mariusz Wojteczek',
                         '[aw,am]': 'Anna Wyrwik | Artur Maszota',
                         '[pd]': 'Paweł Deptuch',
                         '[ms]': 'Mirosław Skrzydło',
                         '[mss]': 'Mirosław Szyłak-Szydłowski',
                         '[pj]': 'Paulina Janota',
                         '[sr]': 'Sebastian Rerak',
                         '[mb]': 'Milena Buszkiewicz lub Maciej Bachorski',
                         '[tm]': 'Tomasz Miecznikowski',
                         '[bs]': 'Błażej Szymankiewicz',
                         '[mw,am]': 'Mariusz Wojteczek | Artur Maszota',
                         '[md]': '[md]',
                         '[em]': '[em]',
                         '[ks]': '[ks]',
                         'Katarzyna Figiel': 'Katarzyna Figiel',
                         'Marcin Waincetel' : 'Marcin Waincetel',
                         'Bartłomiej Paszylk': 'Bartłomiej Paszylk',
                         'Krzysztof Stelmarczyk': 'Krzysztof Stelmarczyk',
                         'Milena Buszkiewicz': 'Milena Buszkiewicz',
                         'Maciej Bachorski': 'Maciej Bachorski',
                         'Natalia Hennig': 'Natalia Hennig',
                         'Dawid Wiktorski':'Dawid Wiktorski',
                         'Ewelina Dyda': 'Ewelina Dyda',
                         'Rafał Siemko': 'Rafał Siemko',
                         'Kasper Linge': 'Kasper Linge',
                         'Łukasz Kamiński': 'Łukasz Kamiński',
                         'Katarzyna Lasek': 'Katarzyna Lasek',
                         'Przemysław Gulda': 'Przemysław Gulda'
                         }

all_articles_links_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_posts, sitemap_links), total=len(sitemap_links)))       

all_results_posts = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(dictionary_of_article, all_articles_links_posts), total=len(all_articles_links_posts)))       


all_articles_links_pages = []
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(get_links_of_sitemap_links_pages, sitemap_links), total=len(sitemap_links)))  



with open(f'booklips_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(all_results, f)        

    
df_posts = pd.DataFrame(all_results_posts).drop_duplicates()
df_posts["Data publikacji"] = pd.to_datetime(df_posts["Data publikacji"]).dt.date
df_posts = df_posts.sort_values('Data publikacji', ascending=False)
   
with pd.ExcelWriter(f'booklips_{datetime.today().date()}.xlsx', engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   


#%%Uploading files on Google Drive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)   
      
upload_file_list = [f'bezprzeginania_{datetime.today().date()}.xlsx', f'bezprzeginania_{datetime.today().date()}.json']
for upload_file in upload_file_list:
	gfile = drive.CreateFile({'parents': [{'id': '19t1szTXTCczteiKfF2ukYsuiWpDqyo8f'}]})  
	gfile.SetContentFile(upload_file)
	gfile.Upload()  


#NOTATKI

#Do analizy częć ksiażki i autorzy - inna struktura artykułów + wiecej informacji - nie mają dat publikacji - przeanalizować wszystklie linki, które zawierą element page
#Autor w recenzji czasem kursywą! 
# POdzielić linki z post i page na dwa arkusze i osobno zeskrobywać 


#2022-11-17
#Zeskrobać częsc z pages (ksiazki, komiksy, autorzy). Najpierw usunac z all_articles_links_posts zbędne linki np. https://booklips.pl/ksiazki/m/ itp. 

#Dodać sleep, bo czasami jest ConnectionError
#Usprawnić pobieranie tytułów i autorów książek z kategorii Recenzja


