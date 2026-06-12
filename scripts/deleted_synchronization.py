import psycopg2
import requests
import time
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

API_HOST = os.getenv('API_HOST')
API_PORT = os.getenv('API_PORT')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
TENANT = os.getenv('TENANT')

BATCH_SIZE = 10
PARALLEL_BATCHES = 4
TOKEN_REFRESH_INTERVAL = 5 * 60  # 5 minutes in seconds

SQL_QUERY = f"""
SELECT external_id FROM {TENANT}_mod_source_record_storage.records_lb record_lb
JOIN {TENANT}_mod_inventory_storage.instance inst
ON record_lb.external_id = inst.id 
WHERE inst.jsonb ->> 'deleted' != 'true' 
AND record_type = 'MARC_BIB'
  AND ((record_lb.leader_record_status = 'd' AND (record_lb.state = 'ACTUAL' OR record_lb.state = 'DELETED')) OR record_lb.state = 'DELETED');
"""

class OkapiSession:
    def __init__(self, host, port, username, password, tenant):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.tenant = tenant
        self.token = None
        self.lock = threading.Lock()
        self.http = requests.Session()
        self.login()
        self.start_token_refresh()

    @staticmethod
    def _extract_token(response):
        # Okapi deployments may return token in headers or response body.
        token = response.headers.get('x-okapi-token') or response.headers.get('X-Okapi-Token')
        if token:
            return token
        try:
            data = response.json()
        except ValueError:
            return None
        return data.get('okapiToken') or data.get('token') or data.get('x-okapi-token')

    def _sync_token_from_cookies(self):
        cookie_token = self.http.cookies.get("folioAccessToken")
        if cookie_token:
            self.token = cookie_token
            return cookie_token
        return self.token

    def login(self):
        url = f"https://{self.host}:{self.port}/authn/login"
        payload = {
            "username": self.username,
            "password": self.password
        }
        headers = {
            "X-Okapi-Tenant": self.tenant,
            "Content-Type": "application/json"
        }
        # Use a persistent session so refresh cookies are stored and reused.
        response = self.http.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            token = self._extract_token(response)
            self.token = token
            # Cookie token is authoritative when present.
            self._sync_token_from_cookies()
            if not self.token:
                raise Exception(f"Login succeeded but no token returned: {response.text}")
            print("Login successful, token acquired.")
        else:
            raise Exception(f"Login failed: {response.text}")

    def refresh_token(self):
        url = f"https://{self.host}:{self.port}/authn/refresh"
        headers = {
            "Content-Type": "application/json",
            "X-Okapi-Tenant": self.tenant
        }
        # Do not send X-Okapi-Token here; refresh uses cookie and some envs require exact match.
        response = self.http.post(url, headers=headers)
        if response.status_code in (200, 201):
            token = self._extract_token(response)
            if token:
                self.token = token
            self._sync_token_from_cookies()
            if self.token:
                print("Token refreshed.")
            else:
                print("Token refresh returned no token; keeping current token.")
        else:
            print(f"Token refresh failed: {response.text}")
            self.login()

    def start_token_refresh(self):
        def refresh_loop():
            while True:
                time.sleep(TOKEN_REFRESH_INTERVAL)
                with self.lock:
                    self.refresh_token()
        threading.Thread(target=refresh_loop, daemon=True).start()

    def get_headers(self):
        with self.lock:
            self._sync_token_from_cookies()
            return {
                "X-Okapi-Tenant": self.tenant,
                "Content-Type": "application/json",
                "X-Okapi-Token": self.token
            }

def get_external_ids():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute(SQL_QUERY)
    results = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def get_instance(session, instance_id):
    url = f"https://{API_HOST}:{API_PORT}/inventory/instances/{instance_id}"
    headers = session.get_headers()
    response = session.http.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"GET failed for {instance_id}: {response.text}")
        return None

def update_instance(session, instance_id, instance_json):
    url = f"https://{API_HOST}:{API_PORT}/inventory/instances/{instance_id}"
    headers = session.get_headers()

    instance_json["deleted"] = True
    instance_json["discoverySuppress"] = True
    instance_json["staffSuppress"] = True
    response = session.http.put(url, json=instance_json, headers=headers)
    if response.status_code == 204:
        print(f"PUT successful for {instance_id}")
    else:
        print(f"PUT failed for {instance_id}: {response.text}")


def process_batch(session, batch_number, batch):
    print(f"Processing batch {batch_number}: {len(batch)} records")
    for instance_id in batch:
        instance_json = get_instance(session, instance_id)
        if instance_json:
            update_instance(session, instance_id, instance_json)

def main():
    start_time = time.perf_counter()
    try:
        session = OkapiSession(API_HOST, API_PORT, USERNAME, PASSWORD, TENANT)
        external_ids = get_external_ids()
        print(f"Total records to update: {len(external_ids)}")

        batches = [
            external_ids[i:i + BATCH_SIZE]
            for i in range(0, len(external_ids), BATCH_SIZE)
        ]

        with ThreadPoolExecutor(max_workers=PARALLEL_BATCHES) as executor:
            futures = [
                executor.submit(process_batch, session, index + 1, batch)
                for index, batch in enumerate(batches)
            ]
            for future in as_completed(futures):
                future.result()
    finally:
        elapsed_seconds = time.perf_counter() - start_time
        print(f"Total run elapsed time: {elapsed_seconds:.2f} seconds")

if __name__ == "__main__":
    main()

