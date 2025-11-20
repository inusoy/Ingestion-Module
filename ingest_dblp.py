import requests
import sqlite3
from models import StandardPaper
from database import get_connection, init_db

class DblpIngestor:
    # Use the .json endpoint directly for better stability
    API_URL = "https://dblp.org/search/publ/api.json"

    def fetch_papers(self, query: str) -> list[StandardPaper]:
        """Queries DBLP and returns a list of StandardPaper objects."""
        params = {
            "q": query,
            "h": 50  # Limit results to 50
        }
        
        # We must identify ourselves to avoid being blocked by DBLP
        headers = {
            "User-Agent": "StudentThesisProject/1.0 (mailto:martin.machnikowski@student.pwr.edu.pl)"
        }
        
        print(f"--> Fetching from DBLP for: '{query}'...")
        try:
            resp = requests.get(self.API_URL, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []

        # Navigate the JSON structure: result -> hits -> hit
        try:
            hits = data['result']['hits']['hit']
        except KeyError:
            print("No hits found.")
            return []

        papers = []
        for hit in hits:
            info = hit['info']
            
            # Handle DBLP quirk: 'authors' can be a dict (single author) or list (multiple)
            author_data = info.get('authors', {}).get('author', [])
            if isinstance(author_data, dict):
                author_list = [author_data['text']]
            elif isinstance(author_data, list):
                # filter out items that don't have 'text' just in case
                author_list = [a['text'] for a in author_data if isinstance(a, dict) and 'text' in a]
            else:
                author_list = []

            # Convert to our Unified Model
            paper = StandardPaper(
                source_id=hit['@id'],
                source_name="dblp",
                title=info.get('title', '').strip('.'),
                authors=author_list,
                year=int(info.get('year', 0)),
                venue=info.get('venue', 'Unknown'),
                doi=info.get('doi', None)
            )
            papers.append(paper)
            
        return papers

    def save_to_db(self, papers: list[StandardPaper]):
        """Saves the unified papers to SQLite."""
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
                # Skip duplicates if they already exist (source_id + source_name must be unique)
                pass
                
        conn.commit()
        conn.close()
        print(f"--> Saved {new_count} new papers (skipped {len(papers) - new_count} duplicates).")

# --- RUNNER ---
if __name__ == "__main__":
    # 1. Make sure DB table exists
    init_db()
    
    # 2. Run Ingestion
    ingestor = DblpIngestor()
    
    # Test 1: Search for your supervisor
    papers = ingestor.fetch_papers("Krystian Wojtkiewicz")
    ingestor.save_to_db(papers)
    
    # Test 2: Search for a topic to get more data
    papers = ingestor.fetch_papers("Data Disambiguation")
    ingestor.save_to_db(papers)