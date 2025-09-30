# fetch_data.py
import requests
import sqlite3
import datetime
import re
import os

# --- Configuration ---
DB_FILE = "poe2_economy.db"
LEAGUE_NAME = "Rise of the Abyssal"

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
    """
    Fetches economic data for a specific league and item overview,
    correctly URL-encoding the parameters.
    """
    base_url = "https://poe.ninja/poe2/api/economy/temp/overview"
    
    # --- FIX: Use a params dictionary for robust URL encoding ---
    params = {
        'leagueName': league_name,
        'overviewName': overview_name
    }
    
    print(f"Fetching data for '{overview_name}' from: {base_url}")
    
    try:
        # Pass the params dictionary to the requests.get() call
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {overview_name}: {e}")
        return None

# --- Helper function for price calculation ---
def calculate_price(rate_value):
    """Calculates the reciprocal of a rate value, handling None or zero."""
    if rate_value is not None and rate_value > 0:
        return 1.0 / rate_value
    return None

# --- Data Processing and Insertion ---
def process_and_insert_data(data, league_name, category_display_name, cursor, conn):
    """
    Processes the PoE 2 JSON data and inserts it into the SQLite database.
    This version now uses the passed-in display_name to ensure consistency with analysis.
    """
    if not data or 'items' not in data:
        print("No valid data to process.")
        return

    current_timestamp = datetime.datetime.now()
    cursor.execute("INSERT OR IGNORE INTO leagues (name) VALUES (?)", (league_name,))
    cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
    league_id = cursor.fetchone()[0]

    # Use the consistent display name from the JS file as the category
    cursor.execute("INSERT OR IGGLISHNORe INTO item_categories (name) VALUES (?)", (category_display_name,))
    cursor.execute("SELECT id FROM item_categories WHERE name = ?", (category_display_name,))
    category_id = cursor.fetchone()[0]

    items_processed = 0
    for item_data in data.get('items', []):
        item_info = item_data.get('item', {})
        # The 'category' field from the API response is now ignored in favor of category_display_name
        if not all(k in item_info for k in ['id', 'name']):
            continue

        cursor.execute("""
        INSERT OR IGNORE INTO items (api_id, name, image_url, category_id)
        VALUES (?, ?, ?, ?)
        """, (item_info['id'], item_info['name'], item_info.get('image'), category_id))
        
        cursor.execute("SELECT id FROM items WHERE api_id = ?", (item_info['id'],))
        item_id_tuple = cursor.fetchone()
        if not item_id_tuple: continue
        item_id = item_id_tuple[0]

        rate_info = item_data.get('rate', {})
        
        chaos_price = calculate_price(rate_info.get('chaos'))
        divine_price = calculate_price(rate_info.get('divine'))
        exalted_price = calculate_price(rate_info.get('exalted'))
        
        cursor.execute("""
        INSERT INTO price_entries (item_id, league_id, timestamp, chaos_value, divine_value, exalted_value)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (item_id, league_id, current_timestamp, chaos_price, divine_price, exalted_price))
        items_processed += 1
    
    conn.commit()
    print(f"Successfully processed and inserted/updated data for {items_processed} items in the '{category_display_name}' category.")
    
# --- Main Execution ---
def main():
    """The main function to run the entire update process."""
    print(f"--- Starting PoE 2 Economy Data Fetch for {LEAGUE_NAME} League ---")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_database_schema(cursor, conn)

    overviews_to_fetch = fetch_all_item_overviews()
    if not overviews_to_fetch:
        print("Halting execution: Could not retrieve item categories.")
        conn.close()
        return

    print(f"\nFound {len(overviews_to_fetch)} categories to process.")
    print("-" * 40)

    for display_name, api_name in overviews_to_fetch:
        print(f"Processing Category: '{display_name}' (using API endpoint: '{api_name}')")
        api_data = fetch_poe_ninja_data(LEAGUE_NAME, api_name)
        if api_data:
            # --- FIX: We are now passing 'display_name' again ---
            process_and_insert_data(api_data, LEAGUE_NAME, display_name, cursor, conn)
        else:
            print(f"Skipping category '{display_name}' due to fetch error or no data.")
        print("-" * 40)
    
    conn.close()
    print("--- Full Process Complete ---")

if __name__ == "__main__":
    main()
