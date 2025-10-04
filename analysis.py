# analysis.py (v18 - SyntaxError Fix)
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
    Uses a parameterized query to prevent SQL injection.
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
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))

def calculate_imputed_values_poe2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a raw dataframe and returns it with an 'imputed_chaos_value' column.
    [FINAL LOGIC] Calculates the Divine rate via the inverse of the Chaos Orb's divine_value,
    as there is no standalone Divine Orb item in the source data.
    """
    divine_to_chaos_rate = None
    exalted_to_chaos_rate = None
    
    # --- Step 1: Correctly find master exchange rates ---
    try:
        # The Divine rate is the INVERSE of the 'divine_value' on the Chaos Orb item.
        chaos_orb_entry = df[df['name'] == 'Chaos Orb'].iloc[0]
        if pd.notna(chaos_orb_entry['divine_value']) and chaos_orb_entry['divine_value'] > 0:
            divine_to_chaos_rate = 1 / chaos_orb_entry['divine_value']

        # The Exalted Orb price is the direct 'chaos_value' from its own row.
        exalted_orb_entry = df[df['name'] == 'Exalted Orb'].iloc[0]
        if pd.notna(exalted_orb_entry['chaos_value']):
            exalted_to_chaos_rate = exalted_orb_entry['chaos_value']
            
        print(f"Rates for analysis: 1 Divine = {divine_to_chaos_rate or 'N/A'}, 1 Exalted = {exalted_to_chaos_rate or 'N/A'}")

    except IndexError as e:
        print(f"Warning: Could not find 'Chaos Orb' or 'Exalted Orb' in the dataset. Imputation will be limited. Error: {e}")

    # --- Step 2: Define imputation logic ---
    def impute_price(row, chaos_rate_col, divine_rate_col, exalted_rate_col):
        # Handle base currencies first. Note: 'Divine Orb' is not in the item list.
        if row['name'] == 'Exalted Orb': return exalted_to_chaos_rate
        if row['name'] == 'Chaos Orb': return 1.0

        chaos_val = row[chaos_rate_col]
        divine_val = row[divine_rate_col]
        exalted_val = row[exalted_rate_col]

        # Priority 1: Use direct chaos value if it exists.
        if pd.notna(chaos_val):
            return chaos_val

        # Priority 2: Impute from Divine value (price_in_divines * divine_to_chaos_rate).
        if pd.notna(divine_val) and pd.notna(divine_to_chaos_rate):
            return divine_val * divine_to_chaos_rate

        # Priority 3: Impute from Exalted value.
        if pd.notna(exalted_val) and pd.notna(exalted_to_chaos_rate):
            return exalted_val * exalted_to_chaos_rate
            
        return None

    # --- Step 3: Apply the logic to create the new columns ---
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

    df_movers = df_analysis[df_analysis['prev_imputed_chaos_value'].notna() & (df_analysis['imputed_chaos_value'] > 10)].copy()
    if not df_movers.empty:
        df_movers = df_movers[df_movers['prev_imputed_chaos_value'] > 0]
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False).dropna(subset=['change'])
        top_gainers = df_movers.head(10); top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
    else:
        top_gainers, top_losers = pd.DataFrame(), pd.DataFrame()
    
    movers_chart_df = pd.concat([top_gainers, top_losers])
    movers_chart_path_str = ""
    if not movers_chart_df.empty:
        fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn', title='Top Market Movers (Last ~24 Hours)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
        movers_chart_path = charts_path / "market_movers.png"; fig_movers.write_image(movers_chart_path, width=1000, height=600)
        movers_chart_path_str = str(movers_chart_path)

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
    
    return market_movers_md, category_md, movers_chart_path_str, str(category_chart_path)

def update_readme(maintenance_md, market_md, category_md, movers_chart, category_chart):
    try:
        with open(README_FILE, 'r', encoding='utf-8') as f: readme_content = f.read()
    except FileNotFoundError:
        readme_content = f"""# PoE Economy Tracker for {LEAGUE_NAME}

<!-- START_MAINTENANCE -->
<!-- END_MAINTENANCE -->

<!-- START_CATEGORY_ANALYSIS -->
<!-- END_CATEGORY_ANALYSIS -->

<!-- START_ANALYSIS -->
<!-- END_ANALYSIS -->"""
    
    new_content = re.sub(r"<!-- START_MAINTENANCE -->.*<!-- END_MAINTENANCE -->", f"<!-- START_MAINTENANCE -->\n{maintenance_md}\n<!-- END_MAINTENANCE -->", readme_content, flags=re.DOTALL)
    full_market_content = f"{market_md}\n\n![Market Movers Chart]({movers_chart})" if movers_chart else market_md
    new_content = re.sub(r"<!-- START_ANALYSIS -->.*<!-- END_ANALYSIS -->", f"<!-- START_ANALYSIS -->\n{full_market_content}\n<!-- END_ANALYSIS -->", new_content, flags=re.DOTALL)
    full_category_content = f"{category_md}\n\n![Category Analysis Chart]({category_chart})" if category_chart else category_md
    new_content = re.sub(r"<!-- START_CATEGORY_ANALYSIS -->.*<!-- END_CATEGORY_ANALYSIS -->", f"<!-- START_CATEGORY_ANALYSIS -->\n{full_category_content}\n<!-- END_ANALYSIS -->", new_content, flags=re.DOTALL)
    
    with open(README_FILE, 'w', encoding='utf-8') as f: f.write(new_content)
    print(f"Successfully updated {README_FILE}")

if __name__ == "__main__":
    print("--- Starting Analysis ---")
    maintenance_table = generate_maintenance_table()
    try:
        conn = sqlite3.connect(DB_FILE)
        df_raw = get_latest_data_df(conn)
        conn.close()
        
        if not df_raw.empty:
            df_imputed = calculate_imputed_values_poe2(df_raw)
            market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
            update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
        else:
            # [SYNTAX FIX] Corrected the unterminated string literal
            update_readme(maintenance_table, "Database is empty or has no recent data.", "Skipping analysis", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")
    print("--- Analysis Complete ---")
