# analysis.py (v20 - Instrumented Debugging)
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
import re
from datetime import datetime

# --- CONFIGURATION ---
DB_FILE = "poe2_economy.db"
LEAGUE_NAME = "Rise of the Abyssal"
CHARTS_DIR = "charts"
README_FILE = "README.md"

def get_latest_data_df(conn) -> pd.DataFrame:
    query = """
    WITH PriceHistory AS (
        SELECT
            p.item_id, p.timestamp,
            p.chaos_value, p.divine_value, p.exalted_value,
            LAG(p.chaos_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_chaos_value,
            LAG(p.divine_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_divine_value,
            LAG(p.exalted_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_exalted_value,
            ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn
        FROM price_entries p
        JOIN leagues l ON p.league_id = l.id
        WHERE l.name = ? AND p.timestamp >= DATETIME('now', '-2 days')
    ),
    LatestPrices AS (
        SELECT * FROM PriceHistory WHERE rn = 1
    )
    SELECT
        i.name, c.name AS category,
        lp.chaos_value, lp.divine_value, lp.exalted_value,
        lp.prev_chaos_value, lp.prev_divine_value, lp.prev_exalted_value
    FROM LatestPrices lp
    JOIN items i ON lp.item_id = i.id
    JOIN item_categories c ON i.category_id = c.id;
    """
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))

def calculate_imputed_values_poe2(df: pd.DataFrame) -> pd.DataFrame:
    divine_to_chaos_rate = None
    exalted_to_chaos_rate = None
    try:
        chaos_orb_entry = df[df['name'] == 'Chaos Orb'].iloc[0]
        if pd.notna(chaos_orb_entry['divine_value']) and chaos_orb_entry['divine_value'] > 0:
            divine_to_chaos_rate = 1 / chaos_orb_entry['divine_value']
        exalted_orb_entry = df[df['name'] == 'Exalted Orb'].iloc[0]
        if pd.notna(exalted_orb_entry['chaos_value']):
            exalted_to_chaos_rate = exalted_orb_entry['chaos_value']
        print(f"Rates for analysis: 1 Divine = {divine_to_chaos_rate or 'N/A'}, 1 Exalted = {exalted_to_chaos_rate or 'N/A'}")
    except IndexError as e:
        print(f"Warning: Could not find 'Chaos Orb' or 'Exalted Orb' in the dataset. Imputation will be limited. Error: {e}")

    def impute_price(row, chaos_rate_col, divine_rate_col, exalted_rate_col):
        if row['name'] == 'Exalted Orb': return exalted_to_chaos_rate
        if row['name'] == 'Chaos Orb': return 1.0
        chaos_val, divine_val, exalted_val = row[chaos_rate_col], row[divine_rate_col], row[exalted_rate_col]
        if pd.notna(chaos_val): return chaos_val
        if pd.notna(divine_val) and pd.notna(divine_to_chaos_rate): return divine_val * divine_to_chaos_rate
        if pd.notna(exalted_val) and pd.notna(exalted_to_chaos_rate): return exalted_val * exalted_to_chaos_rate
        return None

    df['imputed_chaos_value'] = df.apply(lambda r: impute_price(r, 'chaos_value', 'divine_value', 'exalted_value'), axis=1)
    df['prev_imputed_chaos_value'] = df.apply(lambda r: impute_price(r, 'prev_chaos_value', 'prev_divine_value', 'prev_exalted_value'), axis=1)
    return df

def generate_maintenance_table() -> str:
    if not Path(DB_FILE).exists(): return "No database file found."
    with sqlite3.connect(DB_FILE) as conn:
        try:
            latest_run_time = pd.read_sql("SELECT MAX(timestamp) as last_run FROM price_entries", conn).iloc[0]['last_run']
            total_rows = pd.read_sql("SELECT COUNT(*) as count FROM price_entries", conn).iloc[0]['count']
        except (pd.io.sql.DatabaseError, IndexError):
            return "Database is empty or corrupt."
    table = "| Metric | Value |\n|:---|:---|\n"
    table += f"| Last Successful Run (UTC) | `{latest_run_time}` |\n"
    table += f"| Total Price Entries in DB | `{total_rows:,}` |\n"
    return table

def df_to_markdown(dataframe, headers):
    md = f"| {' | '.join(headers)} |\n"
    md += f"|{' :--- |' * len(headers)}\n"
    for _, row in dataframe.iterrows():
        md += f"| {' | '.join(map(str, row))} |\n"
    return md

def generate_analysis_content(df: pd.DataFrame) -> tuple[str, str, str, str]:
    if df.empty or 'imputed_chaos_value' not in df.columns or df['imputed_chaos_value'].isna().all():
        return "Not enough data for analysis.", "Please wait for another run.", "", ""
        
    charts_path = Path(CHARTS_DIR); charts_path.mkdir(exist_ok=True)
    df_analysis = df.dropna(subset=['imputed_chaos_value']).copy()

    # --- START OF DEBUG BLOCK ---
    print("\n--- DETAILED DEBUG FOR generate_analysis_content ---")
    
    # Step 1: Verify the initial state of df_analysis
    print("\n[DEBUG STEP 1] Initial state of the filtered df_analysis DataFrame:")
    print(f"  - Total rows: {len(df_analysis)}")
    print(f"  - Data types:\n{df_analysis.dtypes.to_string()}")
    
    # Step 2: Explicitly check for Mirror of Kalandra
    print("\n[DEBUG STEP 2] Checking for 'Mirror of Kalandra' in df_analysis:")
    mirror_row = df_analysis[df_analysis['name'] == 'Mirror of Kalandra']
    if not mirror_row.empty:
        print("  - SUCCESS: 'Mirror of Kalandra' FOUND in the dataset.")
        print(f"  - Details:\n{mirror_row.to_string()}")
    else:
        print("  - CRITICAL FAILURE: 'Mirror of Kalandra' IS MISSING from df_analysis. This is the root cause.")

    # Step 3: This is the MOST IMPORTANT debug step.
    print("\n[DEBUG STEP 3] Creating 'top_item_per_category' DataFrame...")
    intermediate_df = df_analysis.sort_values(by='imputed_chaos_value', ascending=False).drop_duplicates(subset=['category'], keep='first')
    
    print("\n--- !!! CRITICAL DEBUG OUTPUT !!! ---")
    print("State of the DataFrame AFTER sorting and dropping duplicates:")
    print("This table MUST contain 'Mirror of Kalandra' for the 'Currency' category.")
    print(intermediate_df[['category', 'name', 'imputed_chaos_value']].to_string())
    print("--- END OF CRITICAL DEBUG OUTPUT ---\n")

    # Now we continue with the rest of the original function's logic, using the intermediate_df
    top_item_per_category_base = intermediate_df
    # --- END OF DEBUG BLOCK ---
    
    df_movers = df_analysis[df_analysis['prev_imputed_chaos_value'].notna() & (df_analysis['imputed_chaos_value'] > 10)].copy()
    if not df_movers.empty:
        df_movers = df_movers[df_movers['prev_imputed_chaos_value'] > 0]
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(b
