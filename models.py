from dataclasses import dataclass
from typing import List, Optional
import json

@dataclass
class StandardPaper:
    """
    The unified format for a research paper.
    Matches the fields required for the Dashboard Mockup .
    """
    source_id: str       # The ID from the API (e.g., DOI or DBLP key)
    source_name: str     # 'dblp', 'crossref', or 'orcid'
    title: str
    authors: List[str]   # List of names: ['K. Wojtkiewicz', 'Martin M.']
    year: int
    venue: str           # Journal or Conference name
    doi: Optional[str] = None

    def to_db_tuple(self):
        """Helper to convert object to database tuple"""
        return (
            self.source_id,
            self.source_name,
            self.title,
            json.dumps(self.authors), # Store list as JSON string in SQLite
            self.year,
            self.venue,
            self.doi
        )