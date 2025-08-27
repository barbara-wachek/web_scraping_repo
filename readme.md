# 📚 Skrypty do ekstrakcji i przetwarzania danych bibliograficznych

Repozytorium **web_scraping_repo** zawiera zestaw skryptów, których celem jest **automatyczne pobieranie danych ze stron internetowych** - blogów, serwisów i portali literackich (lub związanych z literaturą) - oraz **przetwarzanie ich do formy tabelarycznej**.  
Każdy wiersz wynikowej tabeli odpowiada jednemu rekordowi bibliograficznemu.

Skrypty powstają w ramach projektu: **„Bibliografia polskiej internetowej kultury cyfrowej wraz z katalogiem źródeł i archiwum. Uzupełnienie »Polskiej Bibliografii Literackiej«”** (NPRH/DN/SP/495736/2021/10), realizowanego w latach **2023–2026**. Wynikiem projektu będzie kolekcja **iPBL**, która zostanie udostępniona na stronach [**Polskiej Bibliografii Literackiej**](https://pbl.ibl.waw.pl/) oraz [**Europejskiej Bibliografii Literackiej**](https://literarybibliography.eu/).

---

## 📂 Struktura repozytorium
- `scripts/` – skrypty do web scrapingu i przetwarzania danych (nazwy plików odpowiadają nazwom scrapowanych stron),  
- `data/` – pliki wynikowe w formatach `.json`, `.xlsx` (udostępniane tylko wewnętrznie)  
- `functions/` – funkcje pomocnicze używane przez skrypty,  

---

### 🔹 Główne biblioteki Python używane w skryptach

#### Do pobierania danych z internetu
- `requests` – pobieranie stron WWW
- `selenium` – automatyzacja przeglądarki i obsługa dynamicznych stron

#### Do parsowania i analizy HTML
- `beautifulsoup4` (`bs4`) – parsowanie HTML i ekstrakcja danych
- `lxml` – parser HTML/XML
- `regex` – zaawansowane dopasowywanie wzorców w tekstach

#### Do przetwarzania danych
- `pandas` – tworzenie tabel, czyszczenie i sortowanie danych

#### Biblioteki pomocnicze
- `tqdm` – wyświetlanie paska postępu przetwarzania
- `datetime` – obsługa dat i czasów (standardowa biblioteka)
- `json` – odczyt i zapis danych w formacie JSON (standardowa biblioteka)
- `concurrent.futures` – równoległe przetwarzanie wątków (ThreadPoolExecutor, standardowa biblioteka)

#### Do zapisu wyników
- `xlsxwriter` lub `openpyxl` – eksport do Excela (`.xlsx`)
