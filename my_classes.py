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