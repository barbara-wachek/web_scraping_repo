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






links_for_scraping = ['https://pgajda.blogspot.com/p/o-autorze.html', 'https://pgajda.blogspot.com/p/bibliografia.html', 'https://pgajda.blogspot.com/p/wycinki.html','https://pgajda.blogspot.com/p/rozmowy.html', 'https://pgajda.blogspot.com/p/recenzje.html', 'https://pgajda.blogspot.com/p/na-skroty.html', 'https://pgajda.blogspot.com/p/spotkania.html']


def scraping_additional_pages(link):
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, 'lxml')
    
    dictionary_of_sections = {}
    
    try:  
        all_content_of_section = soup.find('div', class_='post-body entry-content')
        title_of_section = soup.find('h3', class_='post-title entry-title').text.strip()
        text_of_section = all_content_of_section.text.strip().replace('\n', ' ').replace('\xa0', ' ').replace('  ', ' ')  
        additional_links = all_content_of_section.find_all('a')
        
        
        dictionary_of_sections['Link'] = link
        dictionary_of_sections["Tytuł"] = title_of_section
        dictionary_of_sections["Tekst"] = text_of_section
        dictionary_of_sections["Linki w tekscie"] = ' | '.join([x['href'] for x in additional_links])
        
        
        list_of_images = [x['src'] for x in all_content_of_section.find_all('img')]
        if list_of_images != []:
            dictionary_of_sections['Zdjęcia/Grafika'] = 'TAK'
                
            
        list_of_video = [x['src'] for x in all_content_of_section.find_all('iframe')]
        if list_of_video != []:
            dictionary_of_sections['Filmy'] = 'TAK'
            
            
        
    except AttributeError:
        pass 
    except IndexError:   
        pass


    all_results.append(dictionary_of_sections)
      
      
all_results = []
  
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(scraping_additional_pages,links_for_scraping),total=len(links_for_scraping)))
    
    
df = pd.DataFrame(all_results)
df.to_excel(f"pgajda_dodatki_{datetime.today().date()}.xlsx", encoding='utf-8', index=False)   




