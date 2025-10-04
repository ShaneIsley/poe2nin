# analysis.py (v9 - Final Version with all Corrections and Exalted Orb Base)
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
COMPARATIVE_CURRENCY = "Exalted" # The currency for all final analysis

def get_latest_data_df(conn) -> pd.DataFrame:
    """Gets the most recent and second-most-recent price entries for every unique item."""
    # This function is unchanged but uses a secure parameterized query.
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

def calculate_imputed_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    [REVISED] Takes a raw dataframe and returns it with a new 'imputed_exalted_value' column.
    This version now prioritizes direct Divine -> Exalted exchange rates if they exist.
    """
    divine_to_exalted_rate = None
    chaos_to_exalted_rate = None
    
    # --- Step 1: Find master exchange rates, PRIORITIZING DIRECT RATES ---
    try:
        # We'll need the Exalted Orb's value in Chaos for multiple fallbacks.
        exalted_orb_entry = df[df['name'] == 'Exalted Orb'].iloc[0]
        exalted_in_chaos = exalted_orb_entry['chaos_value']

        # --- Divine to Exalted Rate Discovery ---
        divine_orb_entry = df[df['name'] == 'Divine Orb'].iloc[0]

        # PRIORITY 1: Check for a direct exchange rate first.
        if pd.notna(divine_orb_entry['exalted_value']):
            divine_to_exalted_rate = divine_orb_entry['exalted_value']
            print(f"Found direct Divine -> Exalted rate: {divine_to_exalted_rate}")
        
        # PRIORITY 2 (Fallback): If no direct rate, calculate it indirectly via Chaos.
        elif pd.notna(exalted_in_chaos) and exalted_in_chaos > 0 and pd.notna(divine_orb_entry['chaos_value']):
            divine_in_chaos = divine_orb_entry['chaos_value']
            divine_to_exalted_rate = divine_in_chaos / exalted_in_chaos
            print(f"Calculated indirect Divine -> Exalted rate via Chaos: {divine_to_exalted_rate}")

        # --- Chaos to Exalted Rate Discovery (This logic remains the most reliable) ---
        if pd.notna(exalted_in_chaos) and exalted_in_chaos > 0:
            chaos_to_exalted_rate = 1 / exalted_in_chaos
            print(f"Calculated Chaos -> Exalted rate: {chaos_to_exalted_rate}")

        print(f"Final rates for analysis: 1 Divine = {divine_to_exalted_rate or 'N/A'} Ex, 1 Chaos = {chaos_to_exalted_rate or 'N/A'} Ex")
            
    except (IndexError, ZeroDivisionError) as e:
        print(f"Warning: Could not determine master exchange rates. Imputation may fail. Error: {e}")

    # --- Step 2: Define imputation logic (this function does not need to change) ---
    def impute_value(row, ex_col, div_col, chaos_col):
        # Priority 1: Use the direct Exalted value if it exists.
        if pd.notna(row[ex_col]):
            return row[ex_col]
        
        # Priority 2: Impute from Divine value if no Exalted value.
        if pd.notna(row[div_col]) and pd.notna(divine_to_exalted_rate):
            return row[div_col] * divine_to_exalted_rate

        # Priority 3: Impute from Chaos value as a last resort.
        if pd.notna(row[chaos_col]) and pd.notna(chaos_to_exalted_rate):
            return row[chaos_col] * chaos_to_exalted_rate
            
        return None

    # --- Step 3: Apply the logic to create the new columns ---
    df['imputed_exalted_value'] = df.apply(lambda r: impute_value(r, 'exalted_value', 'divine_value', 'chaos_value'), axis=1)
    df['prev_imputed_exalted_value'] = df.apply(lambda r: impute_value(r, 'prev_exalted_value', 'prev_divine_value', 'prev_chaos_value'), axis=1)
    
    # --- Step 4: Manually set the value for the base currencies themselves for consistency ---
    df.loc[df['name'] == 'Exalted Orb', 'imputed_exalted_value'] = 1.0
    if divine_to_exalted_rate is not None:
        df.loc[df['name'] == 'Divine Orb', 'imputed_exalted_value'] = divine_to_exalted_rate
    if chaos_to_exalted_rate is not None:
        df.loc[df['name'] == 'Chaos Orb', 'imputed_exalted_value'] = chaos_to_exalted_rate
        
    return df

def generate_maintenance_table() -> str:
    # This function is unchanged
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
    # This function is unchanged
    md = f"| {' | '.join(headers)} |\n"
    md += f"|{' :--- |' * len(headers)}\n"
    for _, row in dataframe.iterrows():
        md += f"| {' | '.join(map(str, row))} |\n"
    return md

def generate_analysis_content(df: pd.DataFrame) -> tuple[str, str, str, str]:
    """
    [UPDATED] This function now uses 'imputed_exalted_value' and updates all text to refer to the new currency.
    """
    imputed_col = f'imputed_{COMPARATIVE_CURRENCY.lower()}_value'
    prev_imputed_col = f'prev_imputed_{COMPARATIVE_CURRENCY.lower()}_value'
    
    if df.empty or imputed_col not in df.columns or df[imputed_col].isna().all():
        return "Not enough data for analysis.", "Please wait for another run.", "", ""
        
    charts_path = Path(CHARTS_DIR); charts_path.mkdir(exist_ok=True)
    df_analysis = df.dropna(subset=[imputed_col]).copy()

    # Calculate movers based on items with a reasonable value (e.g., > 0.05 Exalted)
    df_movers = df_analysis[df_analysis[prev_imputed_col].notna() & (df_analysis[imputed_col] > 0.05)].copy()
    if not df_movers.empty:
        df_movers['change'] = ((df_movers[imputed_col] - df_movers[prev_imputed_col]) / df_movers[prev_imputed_col]) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False).dropna(subset=['change'])
        top_gainers = df_movers.head(10); top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
    else:
        top_gainers, top_losers = pd.DataFrame(), pd.DataFrame()
    
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn', 
                        title='Top Market Movers (Last ~24 Hours)', 
                        labels={'name': 'Item', 'change': f'% Change in {COMPARATIVE_CURRENCY} Value'})
    movers_chart_path = charts_path / "market_movers.png"; fig_movers.write_image(movers_chart_path, width=1000, height=600)

    top_valuable = df_analysis.sort_values(by=imputed_col, ascending=False).head(10)[['name', imputed_col]]
    top_valuable[imputed_col] = top_valuable[imputed_col].apply(lambda x: f"{x:,.2f}") # Format for Exalted values
    market_movers_md = f"### Top 10 Most Valuable Items (in {COMPARATIVE_CURRENCY} Orbs)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', f'Imputed {COMPARATIVE_CURRENCY} Value'])
    
    top_item_per_category = df_analysis.loc[df_analysis.groupby('category')[imputed_col].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by=imputed_col, ascending=False)[['category', 'name', imputed_col]].head(15)
    top_item_per_category[imputed_col] = top_item_per_category[imputed_col].apply(lambda x: f"{x:,.2f}")
    
    median_by_category = df_analysis.groupby('category')[imputed_col].median().sort_values(ascending=False).reset_index()
    fig_category = px.bar(median_by_category.head(20), x='category', y=imputed_col, 
                          title='Median Item Value by Category (Top 20)', log_y=True, 
                          labels={'category': 'Item Category', imputed_col: f'Median {COMPARATIVE_CURRENCY} Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"; fig_category.write_image(category_chart_path, width=1000, height=600)
    
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', f'Imputed {COMPARATIVE_CURRENCY} Value'])
    
    return market_movers_md, category_md, str(movers_chart_path), str(category_chart_path)

def update_readme(maintenance_md, market_md, category_md, movers_chart, category_chart):
    # This function is unchanged
    try:
        with open(README_FILE, 'r') as f: readme_content = f.read()
    except FileNotFoundError:
        readme_content = f"# PoE Economy Tracker for {LEAGUE_NAME}\n\n<!-- START_MAINTENANCE -->\n<!-- END_MAINTENANCE -->\n\n<!-- START_CATEGORY_ANALYSIS -->\n<!-- END_CATEGORY_ANALYSIS -->\n\n<!-- START_ANALYSIS -->\n<!-- END_ANALYSIS -->"
    new_content = re.sub(r"<!-- START_MAINTENANCE -->.*<!-- END_MAINTENANCE -->", f"<!-- START_MAINTENANCE -->\n{maintenance_md}\n<!-- END_MAINTENANCE -->", readme_content, flags=re.DOTALL)
    full_market_content = f"{market_md}\n\n![Market Movers Chart]({movers_chart})" if movers_chart else market_md
    new_content = re.sub(r"<!-- START_ANALYSIS -->.*<!-- END_ANALYSIS -->", f"<!-- START_ANALYSIS -->\n{full_market_content}\n<!-- END_ANALYSIS -->", new_content, flags=re.DOTALL)
    full_category_content = f"{category_md}\n\n![Category Analysis Chart]({category_chart})" if category_chart else category_md
    new_content = re.sub(r"<!-- START_CATEGORY_ANALYSIS -->.*<!-- END_CATEGORY_ANALYSIS -->", f"<!-- START_CATEGORY_ANALYSIS -->\n{full_category_content}\n<!-- END_CATEGORY_ANALYSIS -->", new_content, flags=re.DOTALL)
    with open(README_FILE, 'w') as f: f.write(new_content)
    print(f"Successfully updated {README_FILE}")

if __name__ == "__main__":
    print("--- Starting Analysis ---")
    maintenance_table = generate_maintenance_table()
    try:
        conn = sqlite3.connect(DB_FILE)
        df_raw = get_latest_data_df(conn)
        conn.close()
        
        if not df_raw.empty:
            df_imputed = calculate_imputed_values(df_raw) # Call the refactored function
            market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
            update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
        else:
            update_readme(maintenance_table, "Database is empty or has no recent data.", "Skipping analysis.", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")
    print("--- Analysis Complete ---")
