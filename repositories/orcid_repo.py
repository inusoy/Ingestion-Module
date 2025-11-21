import datetime
import random
import hashlib
from database import get_connection

class OrcidRepository:
    def __init__(self):
        pass

    def _generate_id(self):
        """Generates a random 63-bit BigInt ID."""
        return random.getrandbits(63)

    def _string_to_bigint(self, text):
        """
        WORKAROUND: The SQL schema defines 'relationship' columns as BIGINT, 
        but the data is text (e.g., 'self').
        We hash the string to a positive 63-bit integer to satisfy the schema.
        """
        if not text:
            return None
        # Create a hash and convert to a positive integer within Postgres BigInt range
        return int(hashlib.sha256(text.encode('utf-8')).hexdigest(), 16) % (2**63 - 1)

    def save_full_profile(self, data):
        conn = get_connection()
        cursor = conn.cursor()
        orcid = data['orcid']
        ts = datetime.datetime.now()

        try:
            print(f"--> Saving profile data for {orcid}...")
            
            # 1. Core Profile
            self._save_profile_core(cursor, orcid, data.get('person'), ts)
            
            # 2. Affiliations
            affiliations = []
            if data.get('employments'):
                affiliations.extend(data['employments'].get('affiliation-group', []))
            if data.get('educations'):
                affiliations.extend(data['educations'].get('affiliation-group', []))
            self._save_affiliations(cursor, orcid, affiliations, ts)

            # 3. Fundings
            fundings = data.get('fundings', {}).get('group', [])
            self._save_fundings(cursor, orcid, fundings, ts)

            # 4. Peer Reviews
            peer_reviews = data.get('peer_reviews', {}).get('group', [])
            self._save_peer_reviews(cursor, orcid, peer_reviews, ts)
            
            # 5. Research Resources
            resources = data.get('research_resources', {}).get('group', [])
            self._save_research_resources(cursor, orcid, resources, ts)

            # 6. Works
            works = data.get('works', {}).get('group', [])
            self._save_works(cursor, orcid, works, ts)

            conn.commit()
            print("✅ Data committed successfully.")

        except Exception as e:
            print(f"❌ Critical SQL Error (Rolling back transaction): {e}")
            conn.rollback()
            raise e # Re-raise so we know the script failed
        finally:
            cursor.close()
            conn.close()

    def _save_profile_core(self, cursor, orcid, person_data, ts):
        if not person_data: return

        # Profile (Upsert is safe here because ORCID is the PK)
        cursor.execute("""
            INSERT INTO profile (orcid, last_modified) VALUES (%s, %s)
            ON CONFLICT (orcid) DO UPDATE SET last_modified = EXCLUDED.last_modified;
        """, (orcid, ts))

        # Record Name
        name = person_data.get('name', {})
        if name:
            cursor.execute("DELETE FROM record_name WHERE orcid = %s", (orcid,))
            cursor.execute("""
                INSERT INTO record_name (id, orcid, given_names, family_name, credit_name, last_modified)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (self._generate_id(), orcid, 
                  (name.get('given-names') or {}).get('value'),
                  (name.get('family-name') or {}).get('value'),
                  (name.get('credit-name') or {}).get('value'), ts))

        # Biography
        bio = (person_data.get('biography') or {}).get('content')
        if bio:
            cursor.execute("DELETE FROM biography WHERE orcid = %s", (orcid,))
            cursor.execute("""
                INSERT INTO biography (id, orcid, biography, last_modified) 
                VALUES (%s, %s, %s, %s)
            """, (self._generate_id(), orcid, bio, ts))

        # Email
        cursor.execute("DELETE FROM email WHERE orcid = %s", (orcid,))
        for em in (person_data.get('emails') or {}).get('email', []):
            if em.get('email'):
                cursor.execute("""
                    INSERT INTO email (email_id, orcid, email, last_modified) 
                    VALUES (%s, %s, %s, %s)
                """, (self._generate_id(), orcid, em['email'], ts))
        
        # Other Name
        cursor.execute("DELETE FROM other_name WHERE orcid = %s", (orcid,))
        for on in (person_data.get('other-names') or {}).get('other-name', []):
            if on.get('content'):
                cursor.execute("""
                    INSERT INTO other_name (other_name_id, orcid, display_name, last_modified) 
                    VALUES (%s, %s, %s, %s)
                """, (self._generate_id(), orcid, on['content'], ts))

        # Researcher URL
        cursor.execute("DELETE FROM researcher_url WHERE orcid = %s", (orcid,))
        for url in (person_data.get('researcher-urls') or {}).get('researcher-url', []):
             if url.get('url'):
                cursor.execute("""
                    INSERT INTO researcher_url (id, orcid, url, url_name, last_modified) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (self._generate_id(), orcid, url.get('url', {}).get('value'), url.get('url-name'), ts))

        # Keywords
        cursor.execute("DELETE FROM profile_keyword WHERE orcid = %s", (orcid,))
        for kw in (person_data.get('keywords') or {}).get('keyword', []):
            if kw.get('content'):
                cursor.execute("""
                    INSERT INTO profile_keyword (id, orcid, keywords_name, last_modified) 
                    VALUES (%s, %s, %s, %s)
                """, (self._generate_id(), orcid, kw['content'], ts))
        
        # Address
        cursor.execute("DELETE FROM address WHERE orcid = %s", (orcid,))
        for addr in (person_data.get('addresses') or {}).get('address', []):
            country_code = (addr.get('country') or {}).get('value')
            country_id = self._get_country_id(cursor, country_code)
            cursor.execute("""
                INSERT INTO address (id, orcid, country_id, last_modified) 
                VALUES (%s, %s, %s, %s)
            """, (self._generate_id(), orcid, country_id, ts))

        # External Identifiers
        cursor.execute("DELETE FROM profile_external_identifier WHERE orcid = %s", (orcid,))
        for eid in (person_data.get('external-identifiers') or {}).get('external-identifier', []):
             cursor.execute("""
                INSERT INTO profile_external_identifier (id, orcid, external_id_reference, external_id_url, last_modified) 
                VALUES (%s, %s, %s, %s, %s)
             """, (self._generate_id(), orcid, eid.get('external-id-value'), eid.get('external-id-url', {}).get('value'), ts))

    def _save_affiliations(self, cursor, orcid, groups, ts):
        # Clean up existing affiliations
        # NOTE: We must handle dependencies if any child tables exist, but currently org_affiliation seems standalone or handled by CASCADE
        # However, the SQL dump doesn't show ON DELETE CASCADE for 'org_affilaition_relation_external_identifier', so we delete that first.
        
        # 1. Find IDs to delete for clean up
        cursor.execute("SELECT id FROM org_affiliation_relation WHERE orcid = %s", (orcid,))
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        if existing_ids:
            # Delete children
            cursor.execute("DELETE FROM org_affilaition_relation_external_identifier WHERE org_affilaition_relation_id = ANY(%s)", (existing_ids,))
            # Delete parent
            cursor.execute("DELETE FROM org_affiliation_relation WHERE orcid = %s", (orcid,))
        
        for group in groups:
            for summary in group.get('summaries', []):
                s = summary.get('employment-summary') or summary.get('education-summary')
                if not s: continue

                try:
                    cursor.execute("SAVEPOINT aff_sp")
                    org_id = self._get_or_create_org(cursor, s.get('organization', {}))
                    
                    start_date = s.get('start-date') or {}
                    end_date = s.get('end-date') or {}
                    
                    s_year = int((start_date.get('year') or {}).get('value', 0) or 0) or None
                    e_year = int((end_date.get('year') or {}).get('value', 0) or 0) or None

                    cursor.execute("""
                        INSERT INTO org_affiliation_relation 
                        (id, orcid, org_id, start_year, end_year, org_affiliation_relation_title, department, last_modified)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (self._generate_id(), orcid, org_id, s_year, e_year, s.get('role-title'), s.get('department-name'), ts))
                    cursor.execute("RELEASE SAVEPOINT aff_sp")
                except Exception as e:
                    print(f"⚠️ Skipping affiliation (Error: {e})")
                    cursor.execute("ROLLBACK TO SAVEPOINT aff_sp")

    def _save_fundings(self, cursor, orcid, groups, ts):
        # Clean up children first
        cursor.execute("SELECT id FROM profile_funding WHERE orcid = %s", (orcid,))
        f_ids = [row[0] for row in cursor.fetchall()]
        if f_ids:
            cursor.execute("DELETE FROM profile_funding_contributor WHERE profile_funding_id = ANY(%s)", (f_ids,))
            cursor.execute("DELETE FROM profile_funding_external_identifier WHERE profile_funding_id = ANY(%s)", (f_ids,))
            cursor.execute("DELETE FROM profile_funding WHERE orcid = %s", (orcid,))
        
        for group in groups:
            for s in group.get('funding-summary', []):
                try:
                    cursor.execute("SAVEPOINT fund_sp")
                    
                    start_date = s.get('start-date') or {}
                    s_year = int((start_date.get('year') or {}).get('value', 0) or 0) or None
                    
                    amount_str = (s.get('amount') or {}).get('value')
                    amount = float(amount_str) if amount_str else None
                    
                    org_id = self._get_or_create_org(cursor, s.get('organization', {}))

                    cursor.execute("""
                        INSERT INTO profile_funding 
                        (id, orcid, title, type, start_year, numeric_amount, currency_code, org_id, last_modified)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (self._generate_id(), orcid, 
                          (s.get('title') or {}).get('title', {}).get('value'), 
                          s.get('type'), 
                          s_year, amount, 
                          (s.get('amount') or {}).get('currency-code'), org_id, ts))
                    cursor.execute("RELEASE SAVEPOINT fund_sp")
                except Exception as e:
                    print(f"⚠️ Skipping funding (Error: {e})")
                    cursor.execute("ROLLBACK TO SAVEPOINT fund_sp")

    def _save_peer_reviews(self, cursor, orcid, groups, ts):
        # Clean up children first
        cursor.execute("SELECT id FROM peer_review WHERE orcid = %s", (orcid,))
        pr_ids = [row[0] for row in cursor.fetchall()]
        if pr_ids:
            cursor.execute("DELETE FROM peer_review_external_identifier WHERE peer_review_id = ANY(%s)", (pr_ids,))
            cursor.execute("DELETE FROM peer_review WHERE orcid = %s", (orcid,))

        for group in groups:
            for s in group.get('peer-review-summary', []):
                try:
                    cursor.execute("SAVEPOINT peer_sp")
                    org_id = self._get_or_create_org(cursor, s.get('convening-organization', {}))
                    
                    # Explicit column naming to avoid mismatch
                    cursor.execute("""
                        INSERT INTO peer_review (id, orcid, org_id, subject_name, last_modified)
                        VALUES (%s, %s, %s, %s, %s) 
                    """, (self._generate_id(), orcid, org_id, (s.get('review-group-id') or '')[:1000], ts))
                    cursor.execute("RELEASE SAVEPOINT peer_sp")
                except Exception as e:
                    print(f"⚠️ Skipping peer review (Error: {e})")
                    cursor.execute("ROLLBACK TO SAVEPOINT peer_sp")

    def _save_research_resources(self, cursor, orcid, groups, ts):
        # Clean up children first
        cursor.execute("SELECT id FROM research_resource WHERE orcid = %s", (orcid,))
        rr_ids = [row[0] for row in cursor.fetchall()]
        if rr_ids:
            cursor.execute("DELETE FROM research_resource_item WHERE research_resource_id = ANY(%s)", (rr_ids,))
            cursor.execute("DELETE FROM research_resource_external_identifier WHERE research_resource_id = ANY(%s)", (rr_ids,))
            cursor.execute("DELETE FROM research_resource WHERE orcid = %s", (orcid,))

        for group in groups:
             for s in group.get('research-resource-summary', []):
                try:
                    cursor.execute("SAVEPOINT res_sp")
                    title = (s.get('title') or {}).get('title', {}).get('value')
                    cursor.execute("""
                        INSERT INTO research_resource (id, orcid, title, last_modified)
                        VALUES (%s, %s, %s, %s)
                    """, (self._generate_id(), orcid, title, ts))
                    cursor.execute("RELEASE SAVEPOINT res_sp")
                except Exception as e:
                    print(f"⚠️ Skipping research resource (Error: {e})")
                    cursor.execute("ROLLBACK TO SAVEPOINT res_sp")

    def _save_works(self, cursor, orcid, groups, ts):
        """
        STRATEGY CHANGE: 
        We cannot use 'put-code' as work_id because it is not globally unique (only unique per orcid).
        We cannot use 'ON CONFLICT' because we are generating random IDs.
        Therefore, we perform a full DELETE of works for this ORCID before inserting.
        """
        
        # 1. Get current work IDs for this user to safely delete children first
        cursor.execute("SELECT work_id FROM work WHERE orcid = %s", (orcid,))
        existing_work_ids = [row[0] for row in cursor.fetchall()]

        if existing_work_ids:
            # 2. Delete children tables (Manually, because SQL schema lacks ON DELETE CASCADE)
            cursor.execute("DELETE FROM work_external_identifier WHERE work_id = ANY(%s)", (existing_work_ids,))
            cursor.execute("DELETE FROM work_contributor WHERE work_id = ANY(%s)", (existing_work_ids,))
            
            # 3. Delete the parent works
            cursor.execute("DELETE FROM work WHERE orcid = %s", (orcid,))

        # 4. Insert new works
        for group in groups:
            for s in group.get('work-summary', []):
                try:
                    cursor.execute("SAVEPOINT work_sp")
                    
                    # Generate a GLOBAL UNIQUE ID, do not use put-code
                    w_id = self._generate_id()
                    
                    title = (s.get('title') or {}).get('title', {}).get('value')
                    venue = (s.get('journal-title') or {}).get('value') 
                    
                    type_str = s.get('type')
                    type_id = self._get_work_type_id(cursor, type_str)
                    
                    cursor.execute("""
                        INSERT INTO work (work_id, title, journal_title, orcid, work_type_id, last_modified)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (w_id, title, venue, orcid, type_id, ts))

                    # External IDs
                    for ext in (s.get('external-ids') or {}).get('external-id', []):
                        rel_name = ext.get('external-id-relationship') or 'self'
                        # Use the hash-based ID lookup
                        rel_id = self._get_relationship_id(cursor, rel_name)

                        cursor.execute("""
                            INSERT INTO work_external_identifier (work_id, type, value, url, relationship_id)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (w_id, ext.get('external-id-type'), ext.get('external-id-value'), ext.get('external-id-url', {}).get('value'), rel_id))
                    
                    cursor.execute("RELEASE SAVEPOINT work_sp")
                except Exception as e:
                    # print(f"⚠️ Skipping work {s.get('put-code')}: {e}")
                    cursor.execute("ROLLBACK TO SAVEPOINT work_sp")

    def _get_or_create_org(self, cursor, org_data):
        name = org_data.get('name')
        if not name: return None
        
        addr = org_data.get('address') or {}
        city = addr.get('city')
        region = addr.get('region')
        country_code = addr.get('country')
        country_id = self._get_country_id(cursor, country_code)

        cursor.execute("SELECT id FROM org WHERE name = %s AND (city = %s OR city IS NULL)", (name, city))
        res = cursor.fetchone()
        if res: return res[0]

        new_id = self._generate_id()
        cursor.execute("""
            INSERT INTO org (id, name, city, region, country_id, date_created) 
            VALUES (%s, %s, %s, %s, %s, NOW()) 
            RETURNING id
        """, (new_id, name, city, region, country_id))
        return cursor.fetchone()[0]

    def _get_country_id(self, cursor, iso2_code):
        if not iso2_code: return None
        
        cursor.execute("SELECT id FROM country WHERE iso2_code = %s", (iso2_code,))
        res = cursor.fetchone()
        if res: return res[0]
        
        try:
            cursor.execute("SAVEPOINT country_insert")
            cursor.execute("INSERT INTO country (iso2_code) VALUES (%s) RETURNING id", (iso2_code,))
            cid = cursor.fetchone()[0]
            cursor.execute("RELEASE SAVEPOINT country_insert")
            return cid
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT country_insert")
            cursor.execute("SELECT id FROM country WHERE iso2_code = %s", (iso2_code,))
            res = cursor.fetchone()
            return res[0] if res else None

    def _get_work_type_id(self, cursor, type_str):
        if not type_str: return None
        
        cursor.execute("SELECT id FROM work_type WHERE work_type = %s", (type_str,))
        res = cursor.fetchone()
        if res: return res[0]
        
        try:
            cursor.execute("SAVEPOINT work_type_insert")
            cursor.execute("INSERT INTO work_type (work_type) VALUES (%s) RETURNING id", (type_str,))
            tid = cursor.fetchone()[0]
            cursor.execute("RELEASE SAVEPOINT work_type_insert")
            return tid
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT work_type_insert")
            cursor.execute("SELECT id FROM work_type WHERE work_type = %s", (type_str,))
            res = cursor.fetchone()
            return res[0] if res else None

    def _get_relationship_id(self, cursor, rel_name):
        """
        Schema safe version: 
        Generates a BigInt hash for the relationship string because
        the database expects a BigInt in the 'relationship' column.
        """
        if not rel_name: return None
        
        # Generate a deterministic number for this string
        rel_val_as_int = self._string_to_bigint(rel_name)

        # Check if we already have an ID for this 'value'
        cursor.execute("SELECT id FROM external_id_relationship WHERE relationship = %s", (rel_val_as_int,))
        res = cursor.fetchone()
        if res: return res[0]

        try:
            cursor.execute("SAVEPOINT rel_insert")
            # Insert the generated integer into the 'relationship' column
            cursor.execute("INSERT INTO external_id_relationship (relationship) VALUES (%s) RETURNING id", (rel_val_as_int,))
            rid = cursor.fetchone()[0]
            cursor.execute("RELEASE SAVEPOINT rel_insert")
            return rid
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT rel_insert")
            cursor.execute("SELECT id FROM external_id_relationship WHERE relationship = %s", (rel_val_as_int,))
            res = cursor.fetchone()
            return res[0] if res else None