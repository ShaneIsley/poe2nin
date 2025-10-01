# analysis.py (v3 - Imputation at Analysis Time)
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

def generate_maintenance_table() -> str:
    # ... (This function is unchanged)
    if not Path(DB_FILE).exists():
        return "No database file found."
    with sqlite3.connect(DB_FILE) as conn:
        latest_run_time = pd.read_sql("SELECT MAX(timestamp) as last_run FROM price_entries", conn).iloc[0]['last_run']
        total_rows = pd.read_sql("SELECT COUNT(*) as count FROM price_entries", conn).iloc[0]['count']
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
    """Takes a pre-processed DataFrame and generates all markdown and chart assets."""
    if df.empty or 'imputed_chaos_value' not in df.columns:
        return "Not enough data for analysis.", "", "", ""
        
    charts_path = Path(CHARTS_DIR)
    charts_path.mkdir(exist_ok=True)

    # --- Market Movers Analysis ---
    df_movers = df[df['prev_imputed_chaos_value'].notna() & (df['imputed_chaos_value'] > 10)].copy()
    if not df_movers.empty:
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False)
        top_gainers = df_movers.head(10)
        top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
    else:
        top_gainers, top_losers = pd.DataFrame(), pd.DataFrame()
    
    # Generate Movers Chart
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change',
                        color_continuous_scale='RdYlGn', title='Top Market Movers (Last ~24 Hours)',
                        labels={'name': 'Item', 'change': '% Change in Chaos Value'})
    fig_movers.update_layout(xaxis={'categoryorder':'total descending'})
    movers_chart_path = charts_path / "market_movers.png"
    fig_movers.write_image(movers_chart_path, width=1000, height=600)

    # Generate Movers Markdown
    top_valuable = df.sort_values(by='imputed_chaos_value', ascending=False).head(10)[['name', 'imputed_chaos_value']]
    top_valuable['imputed_chaos_value'] = top_valuable['imputed_chaos_value'].round(1)
    
    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Imputed Chaos Value'])

    # --- Category Analysis ---
    top_item_per_category = df.loc[df.groupby('category')['imputed_chaos_value'].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by='imputed_chaos_value', ascending=False)
    top_item_per_category = top_item_per_category[['category', 'name', 'imputed_chaos_value']].head(15)
    top_item_per_category['imputed_chaos_value'] = top_item_per_category['imputed_chaos_value'].round(1)

    median_by_category = df.groupby('category')['imputed_chaos_value'].median().sort_values(ascending=False).reset_index()
    
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
    try:
        with open(README_FILE, 'r') as f:
            readme_content = f.read()
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
    
    conn = sqlite3.connect(DB_FILE)
    df_raw = get_latest_data_df(conn)
    df_imputed = calculate_imputed_values(df_raw)
    conn.close()

    if not df_imputed.empty:
        market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
        update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
    else:
        print("Skipping README update due to insufficient data for analysis.")

    print("--- Analysis Complete ---")
