from database import init_db
from clients.orcid_client import OrcidClient
from repositories.orcid_repo import OrcidRepository

def run_ingestion():
    # 1. Setup
    init_db()
    client = OrcidClient()
    repo = OrcidRepository()

    # 2. Search
    target_name = "Krystian Wojtkiewicz"
    print(f"--- Starting Ingestion for: {target_name} ---")
    
    orcid_id = client.get_orcid_id(target_name)
    if not orcid_id:
        print("❌ Person not found.")
        return

    print(f"✅ Found ORCID: {orcid_id}")

    # 3. Fetch All Data
    print("--> Downloading full profile from API...")
    profile_data = client.get_full_profile(orcid_id)

    # 4. Save to DB
    print("--> Saving to PostgreSQL...")
    repo.save_full_profile(profile_data)
    print("🎉 Ingestion Complete!")

if __name__ == "__main__":
    run_ingestion()