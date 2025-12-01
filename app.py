import streamlit as st
import pandas as pd
import asyncio
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from streamlit_js_eval import streamlit_js_eval
import sys
from st_aggrid import AgGrid, GridOptionsBuilder
import threading
import os

# Import the async orchestrator so we can start it when the app launches
try:
    from scripts.generate_scores import auto_refresh_scraper
except Exception as e:
    auto_refresh_scraper = None
    print(f"Warning: could not import auto_refresh_scraper: {e}")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

try:
    buy_df = pd.read_csv("data/buy_recommendations.csv")
except (pd.errors.EmptyDataError, FileNotFoundError):
    buy_df = pd.DataFrame(columns=["ticker", "score", "confidence", "hedge_fund_count", "insider_buy_count", "congress_buy_count"])

try:
    sell_df = pd.read_csv("data/sell_recommendations.csv")
except (pd.errors.EmptyDataError, FileNotFoundError):
    sell_df = pd.DataFrame(columns=["ticker", "score", "confidence", "insider_sell_count", "congress_sell_count"])

if 'auto_refresh_started' not in st.session_state:
    st.session_state['auto_refresh_started'] = False

if not st.session_state['auto_refresh_started'] and auto_refresh_scraper is not None:
    SCRAPER_INTERVAL_MINUTES = 5  # <- change this value as needed
    interval = SCRAPER_INTERVAL_MINUTES

    def _run_scraper():
        try:
            asyncio.run(auto_refresh_scraper(interval))
        except Exception as e:
            print(f"auto_refresh_scraper terminated with exception: {e}")

    t = threading.Thread(target=_run_scraper, daemon=True)
    t.start()
    st.session_state['auto_refresh_started'] = True

user_tz = streamlit_js_eval(js_expressions="Intl.DateTimeFormat().resolvedOptions().timeZone")

if user_tz is None:
    user_tz = "UTC"


utcTime = datetime.datetime.fromtimestamp(Path("data/buy_recommendations.csv").stat().st_mtime, tz=datetime.timezone.utc)

local_tz = ZoneInfo(user_tz)
localTime = utcTime.astimezone(local_tz)
lastRefreshString = localTime.strftime("%Y-%m-%d %H:%M:%S")

st.set_page_config(page_title="SmartPredict", layout="wide")

css = """
<style>
.block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
}
    
/* Remove extra space around title */
h1 {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

@import url('https://fonts.googleapis.com/css2?family=Open+Sans&display=swap');

html, body, [class*="css"]  {
    font-family: 'Open Sans', sans-serif;
}

.centered-header .ag-header-cell-label {
    justify-content: center !important;
    text-align: center !important;
}

</style>
"""

st.markdown(css, unsafe_allow_html=True)


from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=60 * 1000, key = "datarefresh")

#20c94728a745
st.markdown('''
<h1 style="margin-top: 0; margin-bottom: 1rem;">
    <span style="color: #239cd9;">Smart</span><span style="color: #20c947;">Predict</span>
</h1>
''', unsafe_allow_html=True)
st.markdown("Stock recommendations based on Insider Trades, Hedge Fund Holdings, and Congress Activity")

st.markdown(f"Last refreshed at {lastRefreshString}")

st.write(f"Timezone: {user_tz}")


#List of common ETFs this program is excluding
excluded_etfs = {'SPY', 'QQQ', 'VTI', 'IWM', 'DIA', 'IVV', 'EFA', 'EEM', 'XLF', 'XLK', 'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLB', 'VNQ', 'TLT', 'HYG', 'ARKK'}

buy_filtered_df = buy_df[~buy_df['ticker'].isin(excluded_etfs)].copy()
sell_filtered_df = sell_df[~sell_df['ticker'].isin(excluded_etfs)].copy()

# options = ["Insider Activity", "Congress Activity", "Hedge Fund Activity"]
# activity_filter = st.multiselect("Filter by activity type:", options)

# if "Insider Activity" in activity_filter:
#     buy_filtered_df = buy_filtered_df[buy_filtered_df["insider_buy_count"] > 0]
#     sell_filtered_df = sell_filtered_df[sell_filtered_df["insider_sell_count"] > 0]
# if "Congress Activity" in activity_filter:
#     buy_filtered_df = buy_filtered_df[buy_filtered_df["congress_buy_count"] > 0]
#     sell_filtered_df = sell_filtered_df[sell_filtered_df["congress_sell_count"] > 0]
# if "Hedge Fund Activity" in activity_filter:
#     buy_filtered_df = buy_filtered_df[buy_filtered_df["hedge_fund_buy_count"] > 0]
#     sell_filtered_df = sell_filtered_df[sell_filtered_df["hedge_fund_sell_count"] > 0]

columnColor="#1F1F1F"

cell_style = {
    'border-right': '2px solid #555',
    'textAlign': 'left',
    'display': 'flex',
    'alignItems': 'center',
    'justifyContent': 'left',
    'color': 'white',
    'fontWeight': 'bold',
    'fontSize': '24px',
}

header_style = {
    'textAlign': 'center',
    'font-weight': 'bold',
    'fontSize': '20px',
    'display': 'flex',
    'color': 'white',
    'alignItems': 'center',
    'justifyContent': 'center',
    "background-color" : columnColor
}

# TOP BUYS SECTION
st.markdown("### 🟢 Top Buy Recommendations")
topBuysDf = buy_filtered_df.nlargest(15, 'score')

gb_buys = GridOptionsBuilder.from_dataframe(topBuysDf)
    
gb_buys.configure_column(
    "ticker", 
    header_name="Ticker", 
    tooltipField="Stock ticker symbol", 
    sortable=False, 
    width=60,
    headerClass='centered-header',
    cellStyle={
        **cell_style,
        'background-color': '#28a745',
    },
    headerStyle=header_style
    )
gb_buys.configure_column(
    "confidence", 
    header_name="Confidence",
    tooltipField="Confidence level (Low, Medium, High)", 
    sortable=False, 
    width=120,
    headerClass='centered-header',
    cellStyle={
        **cell_style,
        'background-color': columnColor,
    },
    headerStyle=header_style
    )
gb_buys.configure_column(
    "score", 
    header_name="Score",
    type=["numericColumn"], 
    tooltipField="Overall buy score", 
    sortable=False, 
    width=100,
    headerClass='centered-header',
    valueFormatter="x.toFixed(1)",
    cellStyle={
        **cell_style,
        'background-color': columnColor,
    },
    headerStyle=header_style
    )
# gb_buys.configure_column(
#     "hedge_fund_buy_count", 
#     header_name="HF Buys", 
#     type=["numericColumn"], 
#     tooltipField="Hedge fund buy count", 
#     sortable=False,
#     width=80,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
#     )
# gb_buys.configure_column(
#     "insider_buy_count", 
#     header_name="Insider Buys", 
#     type=["numericColumn"], 
#     tooltipField="Number of insider buys", 
#     sortable=False,
#     width=100,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
#     )
# gb_buys.configure_column(
#     "congress_buy_count", 
#     header_name="Congress Buys", 
#     type=["numericColumn"], 
#     tooltipField="Number of congress buys", 
#     sortable=False,
#     width=110,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
#     )

# Hide buy count columns in buy recommendations
gb_buys.configure_column("hedge_fund_buy_count", hide=True)
gb_buys.configure_column("insider_buy_count", hide=True)
gb_buys.configure_column("congress_buy_count", hide=True)
gb_buys.configure_column("hedge_fund_sell_count", hide=True)
gb_buys.configure_column("insider_sell_count", hide=True)
gb_buys.configure_column("congress_sell_count", hide=True)

gb_buys.configure_grid_options(rowHeight=40)
grid_options_buys = gb_buys.build()
grid_options_buys['suppressMenuHide'] = True
grid_options_buys['suppressExcelExport'] = True
grid_options_buys['suppressCsvExport'] = True
grid_options_buys['enableRangeSelection'] = False
grid_options_buys['statusBar'] = None
grid_options_buys['domLayout'] = 'normal'
grid_options_buys['defaultColDef'] = {
    'cellStyle': {'textAlign': 'center', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}
}

AgGrid(
    topBuysDf,
    gridOptions=grid_options_buys,
    height=500,
    width='100%',
    theme='streamlit',
    fit_columns_on_grid_load=True,
    enable_enterprise_modules=False,
    allow_unsafe_jscode=True,
)

st.markdown("---")  # Separator
st.markdown("### 🔴 Top Sell Recommendations")
topSellsDf = sell_filtered_df.nlargest(15, 'score')

gb_sells = GridOptionsBuilder.from_dataframe(topSellsDf)

gb_sells.configure_column(
    "ticker", 
    header_name="Ticker", 
    tooltipField="Stock ticker symbol", 
    sortable=False, 
    width=60,
    headerClass='centered-header',
    cellStyle={
        **cell_style,
        'background-color': '#dc3545',
    },
    headerStyle=header_style
    )
gb_sells.configure_column(
    "confidence", 
    header_name="Confidence",
    tooltipField="Confidence level (Low, Medium, High)", 
    sortable=False, 
    width=120,
    headerClass='centered-header',
    cellStyle={
        **cell_style,
        'background-color': columnColor,
    },
    headerStyle=header_style
    )
gb_sells.configure_column(
    "score", 
    header_name="Score",
    type=["numericColumn"], 
    tooltipField="Overall sell score", 
    sortable=False, 
    width=100,
    headerClass='centered-header',
    valueFormatter="x.toFixed(1)",
    cellStyle={
        **cell_style,
        'background-color': columnColor,
    },
    headerStyle=header_style
    )
# gb_sells.configure_column(
#     "hedge_fund_sell_count", 
#     header_name="HF Sells", 
#     type=["numericColumn"], 
#     tooltipField="Hedge fund buy count", 
#     sortable=False,
#     width=80,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
# )
# gb_sells.configure_column(
#     "insider_sell_count", 
#     header_name="Insider Sells", 
#     type=["numericColumn"], 
#     tooltipField="Number of insider sells", 
#     sortable=False,
#     width=110,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
#     )
# gb_sells.configure_column(
#     "congress_sell_count", 
#     header_name="Congress Sells", 
#     type=["numericColumn"], 
#     tooltipField="Number of congress sells", 
#     sortable=False,
#     width=120,
#     headerClass='centered-header',
#     cellStyle={
#         **cell_style,
#         'background-color': columnColor,
#     },
#     headerStyle=header_style
#     )

# Hide sell count columns in sell recommendations
gb_sells.configure_column("hedge_fund_sell_count", hide=True)
gb_sells.configure_column("insider_sell_count", hide=True)
gb_sells.configure_column("congress_sell_count", hide=True)
gb_sells.configure_column("hedge_fund_buy_count", hide=True)
gb_sells.configure_column("insider_buy_count", hide=True)
gb_sells.configure_column("congress_buy_count", hide=True)

gb_sells.configure_grid_options(rowHeight=40)
grid_options_sells = gb_sells.build()
grid_options_sells['suppressMenuHide'] = True
grid_options_sells['suppressExcelExport'] = True
grid_options_sells['suppressCsvExport'] = True
grid_options_sells['enableRangeSelection'] = False
grid_options_sells['statusBar'] = None
grid_options_sells['domLayout'] = 'normal'
grid_options_sells['defaultColDef'] = {
    'cellStyle': {'textAlign': 'center', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'}
}

AgGrid(
    topSellsDf,
    gridOptions=grid_options_sells,
    height=500,
    width='100%',
    theme='streamlit',
    fit_columns_on_grid_load=True,
    enable_enterprise_modules=False,
    allow_unsafe_jscode=True,
)

st.markdown("Created by Sanay Tyagi")