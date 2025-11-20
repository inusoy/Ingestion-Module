# Ingestion Module

This project is a data ingestion module designed to fetch academic publication data from multiple external sources (Crossref, DBLP, and ORCID) and unify it into a local SQLite database. It is part of a Student Thesis Project.

## Project Structure

- **`models.py`**: Defines the `StandardPaper` data class, which serves as the unified format for research papers across all sources.
- **`database.py`**: Handles database connections and initialization. It creates a `thesis_data.db` SQLite database with a `papers` table.
- **`ingest_crossref.py`**: Fetches publication data from the [Crossref API](https://api.crossref.org/).
- **`ingest_dblp.py`**: Fetches publication data from the [DBLP API](https://dblp.org/faq/13501473).
- **`ingest_orcid.py`**: Fetches publication data from the [ORCID Public API](https://pub.orcid.org/v3.0).

## Prerequisites

- Python 3.x
- `requests` library

You can install the required library using pip:

```bash
pip install requests
```

## Usage

### 1. Initialize the Database

The database is automatically initialized when running any of the ingestion scripts, but you can also initialize it manually by running:

```bash
python database.py
```

This will create a file named `thesis_data.db` in the project directory.

### 2. Run Ingestion Scripts

Each ingestion script is standalone and includes a default runner that fetches data for specific queries (e.g., "Krystian Wojtkiewicz" or "Data Disambiguation").

To ingest data from **Crossref**:
```bash
python ingest_crossref.py
```

To ingest data from **DBLP**:
```bash
python ingest_dblp.py
```

To ingest data from **ORCID**:
```bash
python ingest_orcid.py
```

### 3. Check the Data

The data is stored in the `papers` table in `thesis_data.db`. You can inspect it using any SQLite viewer or by writing a simple Python script to query the database.

## Database Schema

The `papers` table has the following columns:

- `id`: Primary Key (Auto-increment)
- `source_id`: The unique ID from the source API (e.g., DOI, DBLP key, or ORCID put-code).
- `source_name`: The name of the source ('crossref', 'dblp', 'orcid').
- `title`: Title of the publication.
- `authors_json`: JSON string containing the list of authors.
- `year`: Year of publication.
- `venue`: Journal or Conference name.
- `doi`: Digital Object Identifier (if available).

*Note: The combination of `source_id` and `source_name` is unique to prevent duplicate entries.*
