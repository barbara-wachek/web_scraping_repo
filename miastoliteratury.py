from scrapegraphai.graphs import SmartScraperGraph
import pandas as pd

#%% ustawienia
sitemap_url = "https://miastoliteratury.com/sitemap_index.xml"

prompt = """
For each page listed in this sitemap, extract:
- title of the article
- main text content
- publication date (if available)
- author(s) (if available)
- category or section
- tags (if available)
- external links in the article
- image URLs
Return results as a list of dictionaries with keys:
'Link', 'Data publikacji', 'Autor', 'Tytuł artykułu', 
'Tekst artykułu', 'Kategoria', 'Tagi', 'Linki zewnętrzne', 
'Zdjęcia/Grafika', 'Filmy', 'Linki do zdjęć'
"""

graph_config = {
    "llm": {
        "model": "gpt-3.5-turbo",
        "api_key": "TWÓJ_KLUCZ_API"  # <-- zamień na swój klucz OpenAI
    },
    "verbose": True,
}

#%% tworzenie smart scraper
smart_scraper = SmartScraperGraph(
    prompt=prompt,
    source=sitemap_url,
    config=graph_config
)

#%% uruchomienie scraper
all_results = smart_scraper.run()

#%% zamiana na DataFrame
df_articles = pd.DataFrame(all_results)

#%% sprawdzenie wyników
print(df_articles.head())

#%% zapis do CSV (opcjonalnie)
df_articles.to_csv("miasto_literatury_articles.csv", index=False, encoding="utf-8-sig")