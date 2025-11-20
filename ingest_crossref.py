import requests
import sqlite3
from models import StandardPaper
from database import get_connection, init_db

class CrossrefIngestor:
    # Crossref Public API
    API_URL = "https://api.crossref.org/works"

    def fetch_papers(self, query: str) -> list[StandardPaper]:
        """Queries Crossref and returns a list of StandardPaper objects."""
        
        # Crossref allows searching specifically for authors or general queries.
        # We'll use the general 'query' parameter which covers both.
        params = {
            "query": query,
            "rows": 50,  # Limit results
            # Start looking for journal articles and conference papers primarily
            "filter": "type:journal-article,type:proceedings-article" 
        }
        
        # Crossref requests that we identify ourselves (The "Polite Pool")
        headers = {
            "User-Agent": "StudentThesisProject/1.0 (mailto:martin.machnikowski@student.pwr.edu.pl)"
        }
        
        print(f"--> Fetching from Crossref for: '{query}'...")
        try:
            resp = requests.get(self.API_URL, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []

        try:
            items = data['message']['items']
        except KeyError:
            print("No results found.")
            return []

        papers = []
        for item in items:
            # 1. Extract Title
            raw_title = item.get('title', [])
            title = raw_title[0] if raw_title else "Unknown Title"

            # 2. Extract Authors (Crossref separates given and family names)
            raw_authors = item.get('author', [])
            author_list = []
            for auth in raw_authors:
                given = auth.get('given', '')
                family = auth.get('family', '')
                if family:
                    # Format: "K. Wojtkiewicz" or "Krystian Wojtkiewicz"
                    full_name = f"{given} {family}".strip()
                    author_list.append(full_name)

            # 3. Extract Year (Try published-print, then published-online, then created)
            date_parts = item.get('published-print', {}).get('date-parts')
            if not date_parts:
                date_parts = item.get('published-online', {}).get('date-parts')
            if not date_parts:
                date_parts = item.get('created', {}).get('date-parts')
            
            year = 0
            if date_parts and date_parts[0]:
                year = date_parts[0][0]

            # 4. Extract Venue (Journal or Container title)
            container = item.get('container-title', [])
            venue = container[0] if container else "Unknown Venue"

            # 5. Standardize
            paper = StandardPaper(
                source_id=item.get('DOI', ''), # Crossref uses DOI as the primary ID
                source_name="crossref",
                title=title,
                authors=author_list,
                year=int(year),
                venue=venue,
                doi=item.get('DOI', None)
            )
            papers.append(paper)
            
        return papers

    def save_to_db(self, papers: list[StandardPaper]):
        conn = get_connection()
        cursor = conn.cursor()
        
        new_count = 0
        for p in papers:
            try:
                cursor.execute('''
                    INSERT INTO papers (source_id, source_name, title, authors_json, year, venue, doi)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', p.to_db_tuple())
                new_count += 1
            except sqlite3.IntegrityError:
                pass
                
        conn.commit()
        conn.close()
        print(f"--> Saved {new_count} new papers (skipped {len(papers) - new_count} duplicates).")

# --- RUNNER ---
if __name__ == "__main__":
    init_db()
    ingestor = CrossrefIngestor()
    
    # Test 1: Search for your supervisor
    papers = ingestor.fetch_papers("Krystian Wojtkiewicz")
    ingestor.save_to_db(papers)
    
    # Test 2: Search for a topic
    papers = ingestor.fetch_papers("Data Disambiguation")
    ingestor.save_to_db(papers)