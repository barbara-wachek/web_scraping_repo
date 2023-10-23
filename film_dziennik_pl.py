from bs4 import BeautifulSoup
import requests
import time
import json
from tqdm import tqdm
import regex as re
from datetime import datetime
from time import mktime
import pandas as pd

def date_change_format_long(date_of_publication):
    date = re.sub(r'(.*\,\s)(\d{1,2}\s)(.*)(\s\d{4})', r'\2\3\4', date_of_publication).strip()
    lookup_table = {"stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04", "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08", "września": "09", "października": "10", "listopada": "11", "grudnia": "12"}
    for k, v in lookup_table.items():
        date = date.replace(k, v)
    
    result = time.strptime(date, "%d %m %Y")
    changed_date = datetime.fromtimestamp(mktime(result))   
    new_date = format(changed_date.date())
    return new_date

def extract_details(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
        
    related_topics_div = soup.find('div', id="relatedTopics")
    
    topics = []
    if related_topics_div:
        for span in related_topics_div.find_all('span', class_="relatedTopic"):
            a_tag = span.find('a')
            if a_tag:
                topics.append(a_tag.get('title'))
    
    if '/recenzje/' in url:
        section = 'Recenzje'
    else:
        section = 'Aktualności'
    
    # Extracting title
    title = soup.title.text.split(" - ")[0]

    # Extracting author
    author_div = soup.select_one('.name.nameOfAuthor')
    author = author_div.text.strip() if author_div else "Unknown"

    # Extracting date
    date_div = soup.select_one('.datePublished')
    date = date_div.text.strip() if date_div else "Unknown"
    date = date.split(',')[0]
    date = date_change_format_long(date)
    
    script_tag = soup.find('script', type="application/ld+json")
    data = json.loads(script_tag.string)
    article_body = data.get("articleBody", "Article body not found.")
    article_soup = BeautifulSoup(article_body, 'html.parser')

    list_zobacz_rowniez = []
    for a_tag in article_soup.find_all('a'):
        preceding_text = a_tag.previous_sibling
        if preceding_text and "Zobacz również" in preceding_text:
            link_url = a_tag['href']
            list_zobacz_rowniez.append(link_url)
            a_tag.extract()

    links = []
    tags = []
    for a_tag in article_soup.find_all('a'):
        href = a_tag.get('href')
        text = a_tag.text
        if '/tagi/' in href:
            tags.append(text)
        else:
            links.append(href)
        a_tag.extract()
        
    updated_article_body = article_soup.get_text()

    return {
        'Link': url,
        "Tytuł artykułu": title,
        "Autor": author,
        "Data publikacji": date,
        "Tekst artykułu": updated_article_body,
        'Linki zewnętrzne': links + list_zobacz_rowniez,
        "Tematy":topics,
        "Tagi": tags,
        "Sekcja": section, 
    }

#%%
base_urls =['https://film.dziennik.pl/recenzje', 'https://film.dziennik.pl/news']
all_links = []

for base_url in base_urls:
    current_page = ""
    while True:
        url = base_url + current_page
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
    
        # Extracting links based on the specific structure of the articles
        article_links = soup.select('div.listItem.listItemSolr.itarticle a')
        extracted_links = [link['href'] for link in article_links if 'artykuly' in link['href']]
        all_links.extend(extracted_links)
    
        # Check for the presence of the 'next' link
        next_link = soup.select_one('a.next')
        if next_link:
            # Extract the page number from the 'next' link's href and append it to the base URL
            current_page = "," + next_link['href'].split(',')[-1]
            time.sleep(1)
        else:
            break
    

details_list=[]
for link in tqdm(all_links):
    time.sleep(1)    
    details = extract_details(link)
    details_list.append(details)
    
for idx,elem in enumerate(details_list):
    for key, value in elem.items():
        if isinstance(value, list):
            value = list(set(value))
            details_list[idx][key] = ' | '.join(value)
    
df = pd.DataFrame(details_list).drop_duplicates()
df["Data publikacji"] = pd.to_datetime(df["Data publikacji"]).dt.date
df = df.sort_values('Data publikacji', ascending=False)

with open(f'data/film_dziennik_{datetime.today().date()}.json', 'w', encoding='utf-8') as f:
    json.dump(details_list, f, ensure_ascii=False)  

with pd.ExcelWriter(f"data/film_dziennik_{datetime.today().date()}.xlsx", engine='xlsxwriter', options={'strings_to_urls': False}) as writer:    
    df.to_excel(writer, 'Posts', index=False, encoding='utf-8')   
    writer.save()     
   
