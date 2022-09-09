import requests
import re
from bs4 import BeautifulSoup



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
class DictionaryOfArticle:

    def __init__(self, url, date, author, title, text, tags):
        self.url = url
        self.date = date
        self.author = author
        self.title = title
        self.text = text
        self.tags = tags

    
    def create_dictionary(self):
        dictionary_of_article = {}   
        dictionary_of_article['Link'] = self.url
        dictionary_of_article['Data publikacji'] = self.date
        dictionary_of_article['Tytuł artykułu'] = self.title
        dictionary_of_article['Tekst artykułu'] = self.text
        dictionary_of_article['Autor'] = self.author
        dictionary_of_article['Tagi'] = self.tags

        return dictionary_of_article
    
    
    def __string__(self):
        return dictionary_of_article
    
    
    
article_link = 'https://afront.org.pl/zydowski-olkusz-krzysztofa-kocjana-juz-w-sprzedazy/'

html_text = requests.get(article_link).text
while '429 Too Many Requests' in html_text:
    time.sleep(5)
    html_text = requests.get(article_link).text
soup = BeautifulSoup(html_text, 'lxml')

date_of_publication = soup.find('time', class_= re.compile(r"entry-date")).text
new_date = fun.date_change_format_short(date_of_publication)
text_of_article = soup.find('div', class_='entry-content single-content')
article = text_of_article.text.strip().replace('\n', ' ').replace('\xa0', ' ')
author = " | ".join([x.text for x in text_of_article.find_all('p', attrs={'style':'text-align: right;'})])
title_of_article = soup.find('h1', class_='entry-title').text.replace('\xa0', ' ')
tags = ''.join([x.text.replace('\n','').strip() for x in soup.find_all('span', class_='category-links term-links category-style-normal')])

#links_in_article = [x['href'] for x in text_of_article.find_all('a')]
#dictionary_of_article['Linki zewnętrzne'] = ' | '.join([x for x in links_in_article if not re.findall(r'afront\.org', x)])

dictionary_of_article = DictionaryOfArticle(article_link, new_date, author, title_of_article, article, tags)
dictionary_of_article_new = create_dictionary(dictionary_of_article)
print(dictionary_of_article_new)


dictionary_of_article = DictionaryOfArticle(article_link, new_date, author, title_of_article, article, tags='')
dictionary_of_article_new = create_dictionary(dictionary_of_article)
print(dictionary_of_article_new)




