import requests
import sqlite3
from models import StandardPaper
from database import get_connection, init_db

class OrcidIngestor:
    BASE_URL = "https://pub.orcid.org/v3.0"

    def fetch_papers(self, query: str) -> list[StandardPaper]:
        """
        ORCID Workflow:
        1. Search for a person matching the query.
        2. Get their ORCID ID.
        3. Fetch the list of works for that ID.
        """
        print(f"--> (ORCID) Searching for profile: '{query}'...")
        
        # Step 1: Find the Person
        headers = {
            "Accept": "application/json",
            "User-Agent": "StudentThesisProject/1.0"
        }
        
        search_params = {
            "q": query,
            "rows": 1  # We only take the top matching profile for now
        }
        
        try:
            resp = requests.get(f"{self.BASE_URL}/search", params=search_params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Error searching ORCID: {e}")
            return []

        # Extract ORCID ID
        try:
            results = data.get('result', [])
            if not results:
                print("No ORCID profile found for this name.")
                return []
            
            orcid_id = results[0].get('orcid-identifier', {}).get('path')
            print(f"    Found ORCID ID: {orcid_id}")
        except (KeyError, IndexError):
            print("Could not parse ORCID ID.")
            return []

        # Step 2: Fetch Works for this Person
        print(f"    Fetching works for {orcid_id}...")
        try:
            works_resp = requests.get(f"{self.BASE_URL}/{orcid_id}/works", headers=headers)
            works_resp.raise_for_status()
            works_data = works_resp.json()
        except Exception as e:
            print(f"Error fetching works: {e}")
            return []

        # Step 3: Parse Works
        papers = []
        groups = works_data.get('group', [])
        
        for group in groups:
            summaries = group.get('work-summary', [])
            if not summaries:
                continue
            
            work = summaries[0]
            
            # --- SAFE EXTRACTION LOGIC ---
            
            # A. Extract Title
            # Use (get() or {}) to handle cases where the API returns null
            title_obj = (work.get('title') or {}).get('title')
            title = 'Unknown Title'
            if title_obj:
                title = title_obj.get('value', 'Unknown Title')
            
            # B. Extract Year
            pub_date = work.get('publication-date')
            year = 0
            if pub_date:
                year_obj = pub_date.get('year')
                if year_obj:
                    try:
                        year = int(year_obj.get('value', 0))
                    except (ValueError, TypeError):
                        year = 0
                
            # C. Extract Venue (Journal Title)
            # This was the line causing the error. Fixed with (get() or {})
            venue_obj = work.get('journal-title')
            venue = 'Unknown Venue'
            if venue_obj:
                venue = venue_obj.get('value', 'Unknown Venue')
            if venue is None: venue = "Unknown Venue" # Extra safety

            # D. Extract DOI
            doi = None
            external_ids = (work.get('external-ids') or {}).get('external-id', [])
            for ext in external_ids:
                if ext.get('external-id-type') == 'doi':
                    doi = ext.get('external-id-value')
                    break
            
            # E. Extract Authors
            # ORCID summaries usually imply the profile owner is the author.
            authors_list = [query] 

            paper = StandardPaper(
                source_id=str(work.get('put-code')),
                source_name="orcid",
                title=title,
                authors=authors_list,
                year=year,
                venue=venue,
                doi=doi
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
    ingestor = OrcidIngestor()
    
    # Search for your supervisor
    papers = ingestor.fetch_papers("Krystian Wojtkiewicz")
    ingestor.save_to_db(papers)