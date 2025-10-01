# analysis.py (v5 - Robust Imputation Logic)
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
    # This function is correct and does not need changes.
    query = f"""
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
        WHERE l.name = '{LEAGUE_NAME}' AND p.timestamp >= DATETIME('now', '-2 days')
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
    return pd.read_sql(query, conn)

def calculate_imputed_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a raw dataframe and returns it with new 'imputed_chaos_value' columns.
    This version uses robust logic to handle base currencies and fractional rates.
    """
    # --- Step 1: Find master exchange rates ---
    divine_orb_chaos_value = None
    exalted_orb_chaos_value = None
    try:
        chaos_orb_entry = df[df['name'] == 'Chaos Orb']
        if not chaos_orb_entry.empty and pd.notna(chaos_orb_entry['divine_value'].iloc[0]):
            divine_orb_chaos_value = 1 / chaos_orb_entry['divine_value'].iloc[0]

        exalted_orb_entry = df[df['name'] == 'Exalted Orb']
        if not exalted_orb_entry.empty and pd.notna(exalted_orb_entry['chaos_value'].iloc[0]):
            exalted_orb_chaos_value = exalted_orb_entry['chaos_value'].iloc[0]
            
        print(f"Rates for analysis: 1 Divine = {divine_orb_chaos_value:.1f}c, 1 Exalted = {exalted_orb_chaos_value:.1f}c")
    except (IndexError, ZeroDivisionError):
        print("Warning: Could not determine master exchange rates.")

    # --- Step 2: Define the imputation logic for a single row ---
    def impute_value(row, val_col, div_col, ex_col):
        # --- Special Cases for base currencies ---
        if row['name'] == 'Chaos Orb':
            return 1.0
        if row['name'] == 'Divine Orb':
            return divine_orb_chaos_value
        if row['name'] == 'Exalted Orb':
            return exalted_orb_chaos_value

        chaos_val = row[val_col]
        divine_val = row[div_col]
        exalted_val = row[ex_col]

        # --- Imputation based on available data ---
        imputed_from_div = (1 / divine_val) * divine_orb_chaos_value if pd.notna(divine_val) and divine_orb_chaos_value else None
        imputed_from_ex = (1 / exalted_val) * exalted_orb_chaos_value if pd.notna(exalted_val) and exalted_orb_chaos_value else None

        if pd.notna(chaos_val):
            # If a chaos value exists, we must check if it's a rate or a direct value.
            # A good heuristic: if the chaos value is much larger than an imputed value, it's a rate.
            if imputed_from_div and chaos_val > (imputed_from_div * 1.5): # Add tolerance
                return 1 / chaos_val
            if imputed_from_ex and chaos_val > (imputed_from_ex * 1.5):
                 return 1 / chaos_val
            return chaos_val # It's a direct value
        
        # If no chaos value, use the imputed values in order of preference
        if imputed_from_div:
            return imputed_from_div
        if imputed_from_ex:
            return imputed_from_ex
            
        return None

    # --- Step 3: Apply the logic to create the new columns ---
    df['imputed_chaos_value'] = df.apply(lambda row: impute_value(row, 'chaos_value', 'divine_value', 'exalted_value'), axis=1)
    df['prev_imputed_chaos_value'] = df.apply(lambda row: impute_value(row, 'prev_chaos_value', 'prev_divine_value', 'prev_exalted_value'), axis=1)
    
    return df

# The rest of the script is identical to the previous version and does not need to be changed.
# I am including it here to make this a single, complete file for you to copy.

def generate_maintenance_table() -> str:
    # ... (This function is unchanged)
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

    # --- Market Movers Analysis ---
    df_movers = df_analysis[df_analysis['prev_imputed_chaos_value'].notna() & (df_analysis['imputed_chaos_value'] > 10)].copy()
    if not df_movers.empty:
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False).dropna(subset=['change'])
        top_gainers = df_movers.head(10)
        top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
    else:
        top_gainers, top_losers = pd.DataFrame(), pd.DataFrame()
    
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn',
                        title='Top Market Movers (Last ~24 Hours)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
    movers_chart_path = charts_path / "market_movers.png"; fig_movers.write_image(movers_chart_path, width=1000, height=600)

    top_valuable = df_analysis.sort_values(by='imputed_chaos_value', ascending=False).head(10)[['name', 'imputed_chaos_value']]
    top_valuable['imputed_chaos_value'] = top_valuable['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")
    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Imputed Chaos Value'])
    
    # --- Category Analysis ---
    top_item_per_category = df_analysis.loc[df_analysis.groupby('category')['imputed_chaos_value'].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by='imputed_chaos_value', ascending=False)
    top_item_per_category = top_item_per_category[['category', 'name', 'imputed_chaos_value']].head(15)
    top_item_per_category['imputed_chaos_value'] = top_item_per_category['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")

    median_by_category = df_analysis.groupby('category')['imputed_chaos_value'].median().sort_values(ascending=False).reset_index()
    fig_category = px.bar(median_by_category.head(20), x='category', y='imputed_chaos_value', title='Median Item Value by Category (Top 20)', log_y=True,
                          labels={'category': 'Item Category', 'imputed_chaos_value': 'Median Chaos Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"; fig_category.write_image(category_chart_path, width=1000, height=600)
    
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', 'Imputed Chaos Value'])
    
    return market_movers_md, category_md, str(movers_chart_path), str(category_chart_path)

def update_readme(maintenance_md, market_md, category_md, movers_chart, category_chart):
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
            df_imputed = calculate_imputed_values(df_raw)
            market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
            update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
        else:
            update_readme(maintenance_table, "Database is empty or has no recent data.", "Skipping analysis.", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")
    print("--- Analysis Complete ---")
