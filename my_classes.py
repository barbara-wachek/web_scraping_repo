import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd



#%% classes
#Podstawowy obiekt do scrapowania linków do artykulow znajdujacych sie wewnatrz sitemapy. W przypadku koniecznosci zejscia poziom nizej trzeba ją przebudować albo stworzyc nową

class BasicCrawler:
    def __init__(self, url):
        self.url = url
        
    def __str__(self):
        return re.sub(r'(http.?\:\/\/)(.*)(\/.*)', r'\2', str(self.url)) 

    def get_links(self):
        html_text = requests.get(self.url).text
        soup = BeautifulSoup(html_text, 'lxml')
        links = [e.text for e in soup.find_all('loc')]
        return links
        
    


#site = Crawler('https://szelestkartek.pl/wp-sitemap-posts-post-1.xml')
#articles_links = site.articles_links()
#print(site)



# def czytam_centralnie_web_scraping_sitemap(sitemap):
#     html_text_sitemap = requests.get(sitemap).text
#     soup = BeautifulSoup(html_text_sitemap, 'lxml')
#     links = [e.text for e in soup.find_all('loc')]
#     return links
    
# def get_article_pages(link):    
#     html_text_sitemap = requests.get(link).text
#     soup = BeautifulSoup(html_text_sitemap, 'lxml')
#     sitemap_links = [e.text for e in soup.find_all('loc')]
#     articles_links.extend(sitemap_links)   



#TESTOWA
class Record:

    def __init__(self, url: str, date:str, author:str, title:str, text:str, tags:str, external_links:str, photos_links:str, title_of_spectacle:str, director:str, book_description:str, author_of_book:str, title_of_book:str, publisher:str, year_and_place_of_publication:str, work_description:str, second_title:str, publication_year:str, journal_number_and_year:str, additional_info:str, date_of_edition:str, audio_link:str, transcription:str, category:str, note_about_author:str, note:str, poems_titles:str, title_of_cycle:str, cycle_link:str, related:str): #Dodać więcej kolumn (jak nie ma to będzie None wpisywane?)
        self.url = url
        self.date = date
        self.author = author
        self.title = title
        self.text = text
        self.tags = tags
        self.external_links = external_links
        self.photos_links = photos_links
        self.title_of_spectacle = title_of_spectacle
        self.director = director
        self.book_description = book_description
        self.author_of_book = author_of_book
        self.title_of_book = title_of_book
        self.publisher = publisher
        self.year_and_place_of_publication = year_and_place_of_publication
        self.work_description = work_description
        self.second_title = second_title 
        self.publication_year = publication_year
        self.journal_number_and_year = journal_number_and_year
        self.additional_info = additional_info
        self.date_of_edition = date_of_edition
        self.audio_link = audio_link
        self.transcription = transcription
        self.category = category
        self.note_about_author = note_about_author
        self.note = note
        self.poems_titles = poems_titles
        self.title_of_cycle = title_of_cycle
        self.cycle_link = cycle_link
        self.related = related


    
    def create_dictionary(self):
        dictionary_of_article = {'Link': self.url,
                                 'Data publikacji': self.date, 
                                 'Tytuł artykułu': self.title,
                                 'Tekst artykułu': self.text,
                                 'Autor': self.author,
                                 'Tagi': self.tags
                                 }   
      
        return dictionary_of_article
    
    
    def __string__(self):
        return dictionary_of_article
  





 #Do generowania raportów (uzupełnianie pliku z raportami)
   
class ScrapingReport:
    
    def __init__(self, main_url, number_of_records, date_of_last_record):
        self.main_url = main_url
        self.number_of_records = number_of_records
        self.date_of_last_record = date_of_last_record
        
    def create_scraping_report(self):
        dictionary_of_report = {'Data raportu': datetime.today().date(),
                                'Link': self.main_url,
                                'Liczba rekordów': self.number_of_records,
                                'Data ostatniego rekordu': self.date_of_last_record}
        list_report = [dictionary_of_report]
        df = pd.DataFrame(list_report)
        
        with pd.ExcelWriter(f"raporty_ze_scrapowania.xlsx", mode='ab', engine="openpyxl") as writer:    #Nie działa tak jakbym chciała 
            df.to_excel(writer, index=False, encoding='utf-8')    
            writer.save()     
    
    










#%%


# article_link = 'https://afront.org.pl/zydowski-olkusz-krzysztofa-kocjana-juz-w-sprzedazy/'

# html_text = requests.get(article_link).text
# while '429 Too Many Requests' in html_text:
#     time.sleep(5)
#     html_text = requests.get(article_link).text
# soup = BeautifulSoup(html_text, 'lxml')

# date_of_publication = soup.find('time', class_= re.compile(r"entry-date")).text
# new_date = fun.date_change_format_short(date_of_publication)
# text_of_article = soup.find('div', class_='entry-content single-content')
# article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
# author = " | ".join([x.text for x in text_of_article.find_all('p', attrs={'style':'text-align: right;'})])
# title_of_article = soup.find('h1', class_='entry-title').text.replace('\xa0', ' ')
# tags = ''.join([x.text.replace('\n','').strip() for x in soup.find_all('span', class_='category-links term-links category-style-normal')])

# #links_in_article = [x['href'] for x in text_of_article.find_all('a')]
# #dictionary_of_article['Linki zewnętrzne'] = ' | '.join([x for x in links_in_article if not re.findall(r'afront\.org', x)])

# dictionary_of_article = DictionaryOfArticle(article_link, new_date, author, title_of_article, article, tags)
# dictionary_of_article_new = create_dictionary(dictionary_of_article)
# print(dictionary_of_article_new)


# dictionary_of_article = DictionaryOfArticle(article_link, new_date, author, title_of_article, article, tags='')
# dictionary_of_article_new = create_dictionary(dictionary_of_article)
# print(dictionary_of_article_new)




