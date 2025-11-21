import requests

class OrcidClient:
    BASE_URL = "https://pub.orcid.org/v3.0"
    HEADERS = {"Accept": "application/json", "User-Agent": "StudentThesisProject/1.0"}

    def get_orcid_id(self, query: str):
        """Searches for a person and returns their ORCID ID."""
        print(f"--> Searching for profile: '{query}'...")
        params = {"q": query, "rows": 1}
        try:
            resp = requests.get(f"{self.BASE_URL}/search", params=params, headers=self.HEADERS)
            if resp.status_code == 200:
                results = resp.json().get('result', [])
                if results:
                    return results[0].get('orcid-identifier', {}).get('path')
        except Exception as e:
            print(f"❌ API Error (Search): {e}")
        return None

    def get_full_profile(self, orcid_id: str):
        """
        Fetches data from endpoints required by the database schema.
        """
        print(f"--> Fetching full profile for {orcid_id}...")
        return {
            "orcid": orcid_id,
            "person": self._fetch_endpoint(orcid_id, "person"),
            "works": self._fetch_endpoint(orcid_id, "works"),
            "fundings": self._fetch_endpoint(orcid_id, "fundings"),
            "employments": self._fetch_endpoint(orcid_id, "employments"),
            "educations": self._fetch_endpoint(orcid_id, "educations"),
            "peer_reviews": self._fetch_endpoint(orcid_id, "peer-reviews"),
            "research_resources": self._fetch_endpoint(orcid_id, "research-resources")
        }

    def _fetch_endpoint(self, orcid_id, endpoint):
        try:
            url = f"{self.BASE_URL}/{orcid_id}/{endpoint}"
            resp = requests.get(url, headers=self.HEADERS)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"⚠️ API Warning: Could not fetch {endpoint}: {e}")
        return {}