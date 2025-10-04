# analysis.py (v8 - Reviewed and Corrected)
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
    """
    Gets the most recent and second-most-recent price entries for every unique item.
    [FIXED] Uses a parameterized query to prevent SQL injection.
    """
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
    # Use params argument to safely pass the league name
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))

def calculate_imputed_values_poe2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a raw PoE 2 dataframe and returns it with a new 'imputed_chaos_value' column.
    [REVISED] Implements robust, direct exchange rate discovery and corrects the imputation math.
    """
    divine_to_chaos_rate = None
    exalted_to_chaos_rate = None
    
    # --- Step 1: Robustly find master exchange rates directly from the source ---
    try:
        # Get the Divine Orb's value in Chaos directly from the 'Divine Orb' entry.
        divine_orb_entry = df[df['name'] == 'Divine Orb'].iloc[0]
        if pd.notna(divine_orb_entry['chaos_value']):
            divine_to_chaos_rate = divine_orb_entry['chaos_value']

        # Get the Exalted Orb's value in Chaos directly from the 'Exalted Orb' entry.
        exalted_orb_entry = df[df['name'] == 'Exalted Orb'].iloc[0]
        if pd.notna(exalted_orb_entry['chaos_value']):
            exalted_to_chaos_rate = exalted_orb_entry['chaos_value']
            
        print(f"Rates for analysis: 1 Divine = {divine_to_chaos_rate or 'N/A'}, 1 Exalted = {exalted_to_chaos_rate or 'N/A'}")
    except IndexError as e:
        print(f"Warning: Could not find 'Divine Orb' or 'Exalted Orb' in the dataset to determine exchange rates. Imputation will be limited. Error: {e}")

    # --- Step 2: Define imputation logic for a single row ---
    def impute_value(row, val_col, div_col, ex_col):
        # Priority 1: Use the direct chaos value if it exists and is valid.
        if pd.notna(row[val_col]):
            return row[val_col]
        
        # Priority 2: Impute from Divine value if no chaos value.
        # [CRITICAL FIX] This is now a direct multiplication, not an incorrect reciprocal.
        if pd.notna(row[div_col]) and pd.notna(divine_to_chaos_rate):
            return row[div_col] * divine_to_chaos_rate

        # Priority 3: Impute from Exalted value as a last resort.
        # [CRITICAL FIX] This is also a direct multiplication.
        if pd.notna(row[ex_col]) and pd.notna(exalted_to_chaos_rate):
            return row[ex_col] * exalted_to_chaos_rate
            
        return None # Return None if no price can be determined

    # --- Step 3: Apply the logic to create the new columns ---
    df['imputed_chaos_value'] = df.apply(lambda r: impute_value(r, 'chaos_value', 'divine_value', 'exalted_value'), axis=1)
    df['prev_imputed_chaos_value'] = df.apply(lambda r: impute_value(r, 'prev_chaos_value', 'prev_divine_value', 'prev_exalted_value'), axis=1)
    
    # --- Step 4: Manually set the value for the base currencies themselves ---
    # This is a cleaner approach that ensures they are correctly valued.
    if divine_to_chaos_rate is not None:
        df.loc[df['name'] == 'Divine Orb', 'imputed_chaos_value'] = divine_to_chaos_rate
    if exalted_to_chaos_rate is not None:
        df.loc[df['name'] == 'Exalted Orb', 'imputed_chaos_value'] = exalted_to_chaos_rate
    df.loc[df['name'] == 'Chaos Orb', 'imputed_chaos_value'] = 1.0

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
    # This function is unchanged, it just works with the corrected data.
    if df.empty or 'imputed_chaos_value' not in df.columns or df['imputed_chaos_value'].isna().all():
        return "Not enough data for analysis.", "Please wait for another run.", "", ""
        
    charts_path = Path(CHARTS_DIR); charts_path.mkdir(exist_ok=True)
    df_analysis = df.dropna(subset=['imputed_chaos_value']).copy()

    df_movers = df_analysis[df_analysis['prev_imputed_chaos_value'].notna() & (df_analysis['imputed_chaos_value'] > 10)].copy()
    if not df_movers.empty:
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False).dropna(subset=['change'])
        top_gainers = df_movers.head(10); top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
    else:
        top_gainers, top_losers = pd.DataFrame(), pd.DataFrame()
    
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn', title='Top Market Movers (Last ~24 Hours)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
    movers_chart_path = charts_path / "market_movers.png"; fig_movers.write_image(movers_chart_path, width=1000, height=600)

    top_valuable = df_analysis.sort_values(by='imputed_chaos_value', ascending=False).head(10)[['name', 'imputed_chaos_value']]
    top_valuable['imputed_chaos_value'] = top_valuable['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")
    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Imputed Chaos Value'])
    
    top_item_per_category = df_analysis.loc[df_analysis.groupby('category')['imputed_chaos_value'].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by='imputed_chaos_value', ascending=False)[['category', 'name', 'imputed_chaos_value']].head(15)
    top_item_per_category['imputed_chaos_value'] = top_item_per_category['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")
    
    median_by_category = df_analysis.groupby('category')['imputed_chaos_value'].median().sort_values(ascending=False).reset_index()
    fig_category = px.bar(median_by_category.head(20), x='category', y='imputed_chaos_value', title='Median Item Value by Category (Top 20)', log_y=True, labels={'category': 'Item Category', 'imputed_chaos_value': 'Median Chaos Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"; fig_category.write_image(category_chart_path, width=1000, height=600)
    
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', 'Imputed Chaos Value'])
    
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
    new_content = re.sub(r"<!-- START_CATEGORY_ANALYSIS -->.*<!-- END_CATEGORY_ANALYSIS -->", f"<!-- START_CATEGORY_ANALYSIS -->\n{full_category_content}\n<!-- END_ANALYSIS -->", new_content, flags=re.DOTALL)
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
            df_imputed = calculate_imputed_values_poe2(df_raw) # Call the corrected function
            market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
            update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
        else:
            update_readme(maintenance_table, "Database is empty or has no recent data.", "Skipping analysis.", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")
    print("--- Analysis Complete ---")
