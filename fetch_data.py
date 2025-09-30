# fetch_data.py
import requests
import sqlite3
import datetime
import re
import os

# --- Configuration ---
DB_FILE = "poe2_economy.db"
LEAGUE = "Rise of the Abyssal"

# --- Database Schema ---
def create_database_schema(cursor, conn):
    """Creates the necessary tables if they don't exist."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS leagues (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS item_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, api_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
        image_url TEXT, category_id INTEGER,
        FOREIGN KEY (category_id) REFERENCES item_categories (id)
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER NOT NULL, league_id INTEGER NOT NULL,
        timestamp DATETIME NOT NULL, chaos_value REAL, divine_value REAL, exalted_value REAL,
        volume_chaos REAL, volume_divine REAL, volume_exalted REAL, max_volume_currency TEXT,
        FOREIGN KEY (item_id) REFERENCES items (id), FOREIGN KEY (league_id) REFERENCES leagues (id)
    );""")
    conn.commit()

# --- API Fetching ---
def fetch_all_item_overviews():
    """Fetches and parses the JS file to get all item category API endpoints."""
    url = 'https://poe.ninja/chunk.DXfiI3y6.mjs'
    print(f"Fetching item category list from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        js_content = response.text
        overview_pairs = re.findall(r'name:"([^"]+)",gggCategory:"([^"]+)"', js_content)
        if not overview_pairs:
            print("Could not find any 'name'/'gggCategory' pairs.")
            return []
        print(f"Successfully extracted {len(overview_pairs)} item overview pairs.")
        return overview_pairs
    except requests.exceptions.RequestException as e:
        print(f"Error fetching JavaScript file: {e}")
        return []

def fetch_poe_ninja_data(league_name, overview_name):
    """Fetches economic data for a specific league and item overview."""
    encoded_overview = requests.utils.quote(overview_name)
    url = f"https://poe.ninja/poe2/api/economy/temp/overview?leagueName={league_name}&overviewName={encoded_overview}"
    print(f"Fetching data for '{overview_name}' from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {overview_name}: {e}")
        return None

# --- Data Processing and Insertion ---
def process_and_insert_data(data, league_name, cursor, conn):
    """Processes the JSON data and inserts it into the SQLite database."""
    if not data or 'items' not in data:
        print("No valid data to process.")
        return

    current_timestamp = datetime.datetime.now()
    cursor.execute("INSERT OR IGNORE INTO leagues (name) VALUES (?)", (league_name,))
    cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
    league_id = cursor.fetchone()[0]

    items_processed = 0
    for item_data in data.get('items', []):
        item_info = item_data.get('item', {})
        if not all(k in item_info for k in ['id', 'name', 'category']):
            continue

        category_name = item_info['category']
        cursor.execute("INSERT OR IGNORE INTO item_categories (name) VALUES (?)", (category_name,))
        cursor.execute("SELECT id FROM item_categories WHERE name = ?", (category_name,))
        category_id = cursor.fetchone()[0]

        cursor.execute("""
        INSERT OR IGNORE INTO items (api_id, name, image_url, category_id)
        VALUES (?, ?, ?, ?)
        """, (item_info['id'], item_info['name'], item_info.get('image'), category_id))
        
        cursor.execute("SELECT id FROM items WHERE api_id = ?", (item_info['id'],))
        item_id_tuple = cursor.fetchone()
        if not item_id_tuple: continue
        item_id = item_id_tuple[0]

        rate_info = item_data.get('rate', {})
        volume_info = item_data.get('volumes', {})
        cursor.execute("""
        INSERT INTO price_entries (item_id, league_id, timestamp, chaos_value, divine_value, exalted_value,
        volume_chaos, volume_divine, volume_exalted, max_volume_currency) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, league_id, current_timestamp, rate_info.get('chaos'), rate_info.get('divine'),
              rate_info.get('exalted'), volume_info.get('chaos'), volume_info.get('divine'),
              volume_info.get('exalted'), item_data.get('maxVolumeCurrency')))
        items_processed += 1
    
    conn.commit()
    print(f"Successfully processed and inserted/updated data for {items_processed} items in this category.")

# --- Main Execution ---
def main():
    """The main function to run the entire update process."""
    print("--- Starting PoE Economy Data Fetch ---")
    
    # Establish database connection
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_database_schema(cursor, conn)

    # Fetch the list of all item categories
    overviews_to_fetch = fetch_all_item_overviews()
    if not overviews_to_fetch:
        print("Halting execution: Could not retrieve item categories.")
        conn.close()
        return

    print(f"\nFound {len(overviews_to_fetch)} categories to process.")
    print("-" * 40)

    # Loop through each category, fetch data, and save it
    for display_name, api_name in overviews_to_fetch:
        print(f"Processing Category: '{display_name}' (using API endpoint: '{api_name}')")
        api_data = fetch_poe_ninja_data(LEAGUE, api_name)
        if api_data:
            process_and_insert_data(api_data, LEAGUE, cursor, conn)
        else:
            print(f"Skipping category '{display_name}' due to fetch error or no data.")
        print("-" * 40)
    
    conn.close()
    print("--- Full Process Complete ---")

if __name__ == "__main__":
    main()
