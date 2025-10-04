# fetch_data.py
import requests
import sqlite3
import datetime
import re
import os
import logging
import time
import json

# --- Configuration ---
DB_FILE = "poe2_economy.db"
LEAGUE_NAME = "Rise of the Abyssal"
REQUEST_DELAY = 1.5
DATA_DIR = "data"

# --- FIX: Hardcoded a dictionary of the item categories ---
# This removes the need to scrape the JavaScript file, making the script more reliable.
# Format: { "Display Name": "api_name" }
ITEM_CATEGORY_MAPPINGS = {
    "Currency": "Currency",
    "Fragments": "Fragments",
    "Abyssal Bones": "Abyss",
    "Uncut Gems": "UncutGems",
    "Lineage Gems": "LineageSupportGems",
    "Essences": "Essences",
    "Soul Cores": "Ultimatum",
    "Talismans": "Talismans",
    "Runes": "Runes",
    "Omens": "Ritual",
    "Expedition": "Expedition",
    "Distilled Emotions": "Delirium",
    "Catalysts": "Breach"
}

# --- Helper function for filenames ---
def sanitize_filename(name):
    """Converts a string into a safe filename."""
    name = name.lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_.-]', '', name)
    return f"{name}.json"

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
# --- FIX: Removed the fetch_all_item_overviews() function as it's no longer needed ---

def fetch_poe_ninja_data(league_name, overview_name):
    """
    Fetches economic data for a specific league and item overview,
    correctly URL-encoding the parameters.
    """
    base_url = "https://poe.ninja/poe2/api/economy/temp/overview"
    
    params = {
        'leagueName': league_name,
        'overviewName': overview_name
    }
    
    logging.info(f"Fetching data for '{overview_name}' from: {base_url}")
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching data for {overview_name}: {e}")
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
    """
    if not data or 'items' not in data:
        logging.warning("No valid data to process.")
        return

    current_timestamp = datetime.datetime.now()
    cursor.execute("INSERT OR IGNORE INTO leagues (name) VALUES (?)", (league_name,))
    cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
    league_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO item_categories (name) VALUES (?)", (category_display_name,))
    cursor.execute("SELECT id FROM item_categories WHERE name = ?", (category_display_name,))
    category_id = cursor.fetchone()[0]

    items_processed = 0
    for item_data in data.get('items', []):
        item_info = item_data.get('item', {})
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
    logging.info(f"Successfully processed and inserted/updated data for {items_processed} items in the '{category_display_name}' category.")
    
# --- Main Execution ---
def main():
    """The main function to run the entire update process."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.info(f"--- Starting PoE 2 Economy Data Fetch for {LEAGUE_NAME} League ---")

    league_data_dir = os.path.join(DATA_DIR, LEAGUE_NAME.lower().replace(" ", "_"))
    try:
        os.makedirs(league_data_dir, exist_ok=True)
        logging.info(f"Data will be saved in '{league_data_dir}'")
    except OSError as e:
        logging.critical(f"Failed to create data directory '{league_data_dir}': {e}")
        return
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_database_schema(cursor, conn)

    # --- FIX: Use the hardcoded dictionary directly ---
    overviews_to_fetch = ITEM_CATEGORY_MAPPINGS
    logging.info(f"Processing {len(overviews_to_fetch)} hardcoded categories.")
    logging.info("-" * 40)

    for display_name, api_name in overviews_to_fetch.items():
        logging.info(f"Processing Category: '{display_name}' (using API endpoint: '{api_name}')")
        api_data = fetch_poe_ninja_data(LEAGUE_NAME, api_name)
        
        if api_data:
            filename = sanitize_filename(display_name)
            filepath = os.path.join(league_data_dir, filename)
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(api_data, f, indent=4)
                logging.info(f"Successfully saved raw data to '{filepath}'")
            except IOError as e:
                logging.error(f"Could not write to file '{filepath}': {e}")

            process_and_insert_data(api_data, LEAGUE_NAME, display_name, cursor, conn)
        else:
            logging.warning(f"Skipping category '{display_name}' due to fetch error or no data.")
        
        logging.info(f"Waiting for {REQUEST_DELAY} seconds before next request...")
        time.sleep(REQUEST_DELAY)
        logging.info("-" * 40)
    
    conn.close()
    logging.info("--- Full Process Complete ---")

if __name__ == "__main__":
    main()
