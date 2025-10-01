# analysis.py (v4 - Complete and Corrected)
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

# --- HELPER & DATA LOADING FUNCTIONS ---

def generate_maintenance_table() -> str:
    """Creates a markdown table with script maintenance info."""
    if not Path(DB_FILE).exists():
        return "No database file found."
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

def get_latest_data_df(conn) -> pd.DataFrame:
    """
    Gets the most recent price entry for every unique item,
    and also fetches the previous entry's values for comparison.
    """
    # This query uses a window function (LAG) to get the previous price entry for each item.
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
    Takes a raw dataframe and returns it with a new 'imputed_chaos_value' column.
    This new column is the normalized chaos value for every item.
    """
    # Find the master exchange rates from the data itself.
    try:
        # For PoE2, Divine is the main currency, so we find its value by inverting the Chaos Orb's price in Divines.
        divine_orb_chaos_value = 1 / df[df['name'] == 'Chaos Orb']['divine_value'].iloc[0]
        exalted_orb_chaos_value = df[df['name'] == 'Exalted Orb']['chaos_value'].iloc[0]
        print(f"Rates for analysis: 1 Divine = {divine_orb_chaos_value:.1f}c, 1 Exalted = {exalted_orb_chaos_value:.1f}c")
    except (IndexError, ZeroDivisionError):
        print("Warning: Could not determine master Divine/Exalted exchange rates. Imputation may be incomplete.")
        df['imputed_chaos_value'] = df['chaos_value'] # Fallback
        df['prev_imputed_chaos_value'] = df['prev_chaos_value']
        return df

    # Function to calculate the chaos value for a single row (for current or previous data)
    def impute_value(row, value_col, divine_col, exalted_col):
        if pd.notna(row[value_col]):
            # If a chaos value exists, it might be a rate. Normalize it.
            # Heuristic: If primaryValue > 1 (from API), it's a rate. Since we don't have that here,
            # we'll assume any 'Currency' with a chaos value over 20 is a rate. This is imperfect but effective.
            if row['category'] == 'Currency' and row[value_col] > 20:
                return 1 / row[value_col]
            return row[value_col]
        elif pd.notna(row[divine_col]) and pd.notna(divine_orb_chaos_value):
            return (1 / row[divine_col]) * divine_orb_chaos_value
        elif pd.notna(row[exalted_col]) and pd.notna(exalted_orb_chaos_value):
            return (1 / row[exalted_col]) * exalted_orb_chaos_value
        return None

    df['imputed_chaos_value'] = df.apply(lambda row: impute_value(row, 'chaos_value', 'divine_value', 'exalted_value'), axis=1)
    df['prev_imputed_chaos_value'] = df.apply(lambda row: impute_value(row, 'prev_chaos_value', 'prev_divine_value', 'prev_exalted_value'), axis=1)
    
    return df

# --- ANALYSIS & MARKDOWN/CHART GENERATION ---

def df_to_markdown(dataframe, headers):
    """Converts a pandas DataFrame to a markdown table."""
    md = f"| {' | '.join(headers)} |\n"
    md += f"|{' :--- |' * len(headers)}\n"
    for _, row in dataframe.iterrows():
        md += f"| {' | '.join(map(str, row))} |\n"
    return md

def generate_analysis_content(df: pd.DataFrame) -> tuple[str, str, str, str]:
    """Takes a pre-processed DataFrame and generates all markdown and chart assets."""
    if df.empty or 'imputed_chaos_value' not in df.columns or df['imputed_chaos_value'].isna().all():
        return "Not enough data for analysis.", "", "", ""
        
    charts_path = Path(CHARTS_DIR)
    charts_path.mkdir(exist_ok=True)
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
    
    # Generate Movers Chart
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn',
                        title='Top Market Movers (Last ~24 Hours)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
    fig_movers.update_layout(xaxis={'categoryorder':'total descending'})
    movers_chart_path = charts_path / "market_movers.png"
    fig_movers.write_image(movers_chart_path, width=1000, height=600)

    # Generate Movers Markdown
    top_valuable = df_analysis.sort_values(by='imputed_chaos_value', ascending=False).head(10)[['name', 'imputed_chaos_value']]
    top_valuable['imputed_chaos_value'] = top_valuable['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")

    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Imputed Chaos Value'])
    # ... more markdown for gainers/losers can be added here if desired

    # --- Category Analysis ---
    top_item_per_category = df_analysis.loc[df_analysis.groupby('category')['imputed_chaos_value'].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by='imputed_chaos_value', ascending=False)
    top_item_per_category = top_item_per_category[['category', 'name', 'imputed_chaos_value']].head(15)
    top_item_per_category['imputed_chaos_value'] = top_item_per_category['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")

    median_by_category = df_analysis.groupby('category')['imputed_chaos_value'].median().sort_values(ascending=False).reset_index()
    
    # Generate Category Chart
    fig_category = px.bar(median_by_category.head(20), x='category', y='imputed_chaos_value',
                          title='Median Item Value by Category (Top 20)', log_y=True,
                          labels={'category': 'Item Category', 'imputed_chaos_value': 'Median Chaos Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"
    fig_category.write_image(category_chart_path, width=1000, height=600)
    
    # Generate Category Markdown
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', 'Imputed Chaos Value'])
    
    return market_movers_md, category_md, str(movers_chart_path), str(category_chart_path)

def update_readme(maintenance_md, market_md, category_md, movers_chart, category_chart):
    """Injects all analysis content into the README.md between markers."""
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

# --- MAIN EXECUTION BLOCK ---

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
            print("Database is empty or has no recent data. Skipping analysis.")
            update_readme(maintenance_table, "Not enough data for analysis.", "", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        # Update readme with error status
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")

    print("--- Analysis Complete ---")
