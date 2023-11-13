import sys
import requests as rq
from bs4 import BeautifulSoup as bs
from time import sleep
from time import time
from random import randint
from warnings import warn
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#%%



# MSNBC Wayback machine archive urls
url = 'https://web.archive.org/cdx/search/cdx?url=http://www.ingaiwasiow.pl/&collapse=digest&from=20150218&to=20150220&output=json'
urls = rq.get(url).text
parse_url = json.loads(urls) #parses the JSON from urls.
## Extracts timestamp and original columns from urls and compiles a url list.
url_list = []
for i in range(1,len(parse_url)):
    orig_url = parse_url[i][2]
    tstamp = parse_url[i][1]
    waylink = tstamp+'/'+orig_url
    url_list.append(waylink)
## Compiles final url pattern.
for url in url_list:
    final_url = 'https://web.archive.org/web/'+url
    
    
# Open page
req = rq.get(final_url).text
# parse html using beautifulsoup and store in soup
soup = bs(req,'html.parser')

archive_links = [e['href'] for e in soup.select('#archives-3 a')]

article_links = []
errors = []
# def get_article_links(archive_link):
for archive_link in tqdm(archive_links):
    try:
        # archive_link = 'https://web.archive.org/web/20150219004708/http://www.ingaiwasiow.pl/2011/05'
        session = rq.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
    
        html_text = session.get(archive_link).text
        
        # html_text = rq.get(archive_link).text
        soup = bs(html_text, "html.parser")
        links = [e['href'] for e in soup.select('#content a')]
        article_links.extend(links)
    except rq.exceptions.ConnectionError:
        errors.append(archive_link)


# article_links = []
# with ThreadPoolExecutor() as excecutor:
#     list(tqdm(excecutor.map(get_article_links, archive_links),total=len(archive_links)))
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    