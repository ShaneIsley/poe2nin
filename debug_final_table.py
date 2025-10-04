# debug_final_table.py
# A focused script to definitively debug the "Most Valuable Item by Category" table generation.

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
            p.item_id, p.timestamp, p.chaos_value, p.divine_value, p.exalted_value,
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
        lp.chaos_value, lp.divine_value, lp.exalted_value
    FROM LatestPrices lp
    JOIN items i ON lp.item_id = i.id
    JOIN item_categories c ON i.category_id = c.id;
    """
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
    except IndexError as e:
        print(f"[DEBUG] WARNING: Could not find base currency. Error: {e}")

    def impute_price(row):
        if row['name'] == 'Exalted Orb': return exalted_to_chaos_rate
        if row['name'] == 'Chaos Orb': return 1.0
        if pd.notna(row['chaos_value']): return row['chaos_value']
        if pd.notna(row['divine_value']) and pd.notna(divine_to_chaos_rate): return row['divine_value'] * divine_to_chaos_rate
        if pd.notna(row['exalted_value']) and pd.notna(exalted_to_chaos_rate): return row['exalted_value'] * exalted_to_chaos_rate
        return None
    df['imputed_chaos_value'] = df.apply(impute_price, axis=1)
    return df

# --- MAIN EXECUTION BLOCK ---

if __name__ == "__main__":
    print("--- Starting Final Table Debug Script ---")
    
    # 1. Load and process data
    conn = sqlite3.connect(DB_FILE)
    df_raw = get_latest_data_df(conn)
    conn.close()
    
    if df_raw.empty:
        print("CRITICAL ERROR: The initial SQL query returned no data. Check database for recent entries.")
    else:
        df_imputed = calculate_imputed_values_poe2(df_raw)
        
        # 2. Prepare the analysis DataFrame, replicating the main script's logic
        df_analysis = df_imputed.dropna(subset=['imputed_chaos_value']).copy()
        df_analysis['imputed_chaos_value'] = pd.to_numeric(df_analysis['imputed_chaos_value'])
        
        print(f"\n[DEBUG] df_analysis prepared. Total rows: {len(df_analysis)}. Data types are:\n{df_analysis.dtypes.to_string()}")
        
        # 3. Explicitly check for the Mirror before starting the loop
        mirror_check = df_analysis[df_analysis['name'] == 'Mirror of Kalandra']
        if not mirror_check.empty:
            print(f"\n[DEBUG] Mirror of Kalandra FOUND in df_analysis with value: {mirror_check['imputed_chaos_value'].iloc[0]:,.1f}")
        else:
            print("\n[DEBUG] CRITICAL: Mirror of Kalandra NOT FOUND in df_analysis before the loop.")

        # 4. Replicate the "Robust Logic" loop with heavy instrumentation
        print("\n--- Starting manual selection loop ---")
        top_items_list = []
        unique_categories = sorted(df_analysis['category'].unique()) # Sorted for predictable output
        
        for category in unique_categories:
            print(f"\n--- Processing Category: '{category}' ---")
            df_category = df_analysis[df_analysis['category'] == category]
            
            # Sort the items within this category by value
            df_category_sorted = df_category.sort_values(by='imputed_chaos_value', ascending=False)
            
            # Get the top item
            top_item = df_category_sorted.iloc[0]
            
            # This is the most important debug line
            print(f"  -> Top item selected for this category: '{top_item['name']}' (Value: {top_item['imputed_chaos_value']:.1f})")
            
            top_items_list.append(top_item)
            
        # 5. Assemble and print the final table
        final_df = pd.DataFrame(top_items_list)
        final_df_sorted = final_df.sort_values(by='imputed_chaos_value', ascending=False)
        
        print("\n\n--- !!! FINAL DEBUG OUTPUT: The Generated Table !!! ---\n")
        print("This is the exact table that would be in the README. Compare the 'Currency' row to the loop output above.")
        print(final_df_sorted[['category', 'name', 'imputed_chaos_value']].to_string())
        
        print("\n--- End of Final Table Debug Script ---")
