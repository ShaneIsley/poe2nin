# debug_currency.py
# A temporary, focused script to definitively debug the currency sorting issue.

import sqlite3
import pandas as pd

# --- CONFIGURATION ---
DB_FILE = "poe2_economy.db"
LEAGUE_NAME = "Rise of the Abyssal"

# --- CORE LOGIC (Copied directly from analysis.py) ---

def get_latest_data_df(conn) -> pd.DataFrame:
    """Loads the most recent data for all items."""
    query = """
    WITH PriceHistory AS (
        SELECT
            p.item_id,
            ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn,
            p.chaos_value, p.divine_value, p.exalted_value
        FROM price_entries p
        JOIN leagues l ON p.league_id = l.id
        WHERE l.name = ?
    ),
    LatestPrices AS (
        SELECT * FROM PriceHistory WHERE rn = 1
    )
    SELECT
        i.name, c.name AS category,
        lp.chaos_value, lp.divine_value, lp.exalted_value
    FROM LatestPrices lp
    JOIN items i ON lp.item_id = i.id
    JOIN item_categories c ON i.category_id = c.id;
    """
    # Note: We are intentionally NOT filtering by timestamp to ensure we get Chaos/Exalted Orbs
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))

def calculate_imputed_values_poe2(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates imputed chaos values. This logic is confirmed to be correct."""
    divine_to_chaos_rate = None
    exalted_to_chaos_rate = None
    try:
        chaos_orb_entry = df[df['name'] == 'Chaos Orb'].iloc[0]
        if pd.notna(chaos_orb_entry['divine_value']) and chaos_orb_entry['divine_value'] > 0:
            divine_to_chaos_rate = 1 / chaos_orb_entry['divine_value']
        exalted_orb_entry = df[df['name'] == 'Exalted Orb'].iloc[0]
        if pd.notna(exalted_orb_entry['chaos_value']):
            exalted_to_chaos_rate = exalted_orb_entry['chaos_value']
        print(f"[DEBUG] Rates for analysis: 1 Divine = {divine_to_chaos_rate}, 1 Exalted = {exalted_to_chaos_rate}")
    except IndexError:
        print("[DEBUG] WARNING: Could not find Chaos Orb or Exalted Orb.")

    def impute_price(row):
        if row['name'] == 'Exalted Orb': return exalted_to_chaos_rate
        if row['name'] == 'Chaos Orb': return 1.0
        if pd.notna(row['chaos_value']): return row['chaos_value']
        if pd.notna(row['divine_value']) and pd.notna(divine_to_chaos_rate): return row['divine_value'] * divine_to_chaos_rate
        if pd.notna(row['exalted_value']) and pd.notna(exalted_to_chaos_rate): return row['exalted_value'] * exalted_to_chaos_rate
        return None
    df['imputed_chaos_value'] = df.apply(impute_price, axis=1)
    return df

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    print("--- Starting Currency Debug Script ---")
    
    conn = sqlite3.connect(DB_FILE)
    df_raw = get_latest_data_df(conn)
    conn.close()
    
    df_imputed = calculate_imputed_values_poe2(df_raw)
    
    # Filter for currency and drop rows where calculation failed
    df_currency = df_imputed[df_imputed['category'] == 'Currency'].dropna(subset=['imputed_chaos_value']).copy()

    # Ensure the value column is a numeric type for correct sorting
    df_currency['imputed_chaos_value'] = pd.to_numeric(df_currency['imputed_chaos_value'])
    
    # Sort the DataFrame by the final calculated value
    df_sorted = df_currency.sort_values(by='imputed_chaos_value', ascending=False)
    
    # --- This is the critical output ---
    print("\n\n--- !!! FINAL DEBUG OUTPUT: SORTED CURRENCY DATAFRAME !!! ---\n")
    # Using to_string() ensures the entire DataFrame is printed to the log
    print("This table shows the final, sorted data that should be used for the report.")
    print("If 'Mirror of Kalandra' is at the top here, the sorting logic is correct.")
    print(df_sorted[['name', 'chaos_value', 'divine_value', 'imputed_chaos_value']].to_string())
    print("\n--- End of Currency Debug Script ---")
