import streamlit as st
import pandas as pd
import datetime
import pytz
import plotly.express as px
import time
import random

from lib import init_nav, warm_cache, init_connection
from chart import render_stock_history
from data import get_stock_min, get_trades_second, get_realtime_second

title = "A Real-Time Analytics Platform"

st.set_page_config(
    page_title=title,
    layout="centered",
)

init_nav()

st.markdown(
    f"""
    # {title}
    ## Welcome to SingleStore
    [SingleStore](https://www.singlestore.com) combines data ingestion, fast analytics, and low-latency transactions
    into a single database platform — a powerful combination for financial applications. This demo site illustrates how
    these capabilities can be used to build a real-time analytics platform for stock trading. You will see:

    - SingleStore's pipelines can automatically ingest data in bulk for large-scale analytical queries.
    - SingleStore's analytical query engine supports real-time low-latency aggregations, over both historical and
      up-to-the-millisecond data ingested in real-time.
    - SingleStore's MongoDB-Compatible API, SingleStore Kai, enables easy consumption through any MongoDB client driver
      and enables serious ease-of-use features for JSON data.

    All of the data backing the charts and tables on this site comes from a SingleStore instance running in Snowpark
    Container Services and is retrieved through the PyMongo driver via SingleStore Kai.
    """
)

selectedTicker = "MSFT"
numDays = 30

def get_random_ticker_and_days():
    ticker = random.choice(
        [
            "AAPL",
            "GOOGL",
            "MSFT",
            "AMZN",
            "TSLA",
            "NVDA",
            "SNOW",
            "META",
            "NFLX",
            "SPOT",
            "BRK.B",
            "GME",
            "VZ",
        ]
    )
    days = random.choice([30, 60, 90, 180])
    return ticker, days


if st.button("Random ticker and day range"):
    selectedTicker, numDays = get_random_ticker_and_days()

nytz = pytz.timezone("America/New_York")

now = nytz.localize(datetime.datetime.now())
dff = get_stock_min(
    [selectedTicker], now - datetime.timedelta(days=numDays), now, "Day"
)
fig = render_stock_history(selectedTicker, "Day", dff, "")
st.plotly_chart(fig)
st.caption(
    f"Trading within the last {numDays} days for {selectedTicker}, aggregated on-demand."
)

st.markdown(
    f"""
    ## Markets Closed?
    Outside of regular trading hours, real-time views can be boring. To keep things interesting, you will see an option to use a replaying stream of historical trades as if it were real-time.
    """
)
x_min = 0
x_max = 0
rt = st.radio("Real-time or replay?", ["Real-Time", "Replay"], index=0)
plot = st.empty()
plot.markdown(
    """
    <div style="height: 216px;">&nbsp;</div>
    """,
    unsafe_allow_html=True,
)

if rt == "Real-Time":
    st.caption(
        f"The real-time last 5-minutes of per-second average trade price of {selectedTicker}."
    )
else:
    st.caption(
        f"The replaying market open of April 9th, 2024 for {selectedTicker}."
    )

st.markdown(
    f"""
    ## Let's Get Started
    This Streamlit app is organized into pages. If you don't see the navigation pane, click the icon in the top-left
    corner and check out the other pages.
    """
)

now = nytz.localize(datetime.datetime(2024, 4, 9, 9, 31))
dff = get_trades_second([selectedTicker], now - datetime.timedelta(seconds=60), now)
fig = render_stock_history(selectedTicker, "Hour", dff, "")
st.plotly_chart(fig)
st.caption(
    f"The first 60 seconds of trading on April 9th, 2024 for {selectedTicker}, aggregated on-demand per-second."
)

st.markdown(
    """
    ## Enjoy the Demo
    """
)

client = init_connection()
db = client.stocks

st.markdown(
    """
    The source is available on [GitHub](https://github.com/jasonthorsness/singlestore-stocks-demo).
    """
)

last_timestamp = None
df = pd.DataFrame()

emptyCount = 0
while True:
    new_dfs = get_realtime_second([selectedTicker], last_timestamp, rt == "Real-Time")
    new_df = new_dfs[selectedTicker]

    fullyEmpty = len(new_df) == 0

    if len(new_df) > 0:
        new_df = new_df[:-1]

    if not new_df.empty and (
        last_timestamp == None or last_timestamp < new_df["_id"].max()
    ):
        last_timestamp = new_df["_id"].max()

        df = pd.concat([df, new_df], ignore_index=True)
        df.drop_duplicates(subset="_id", keep="last", inplace=True)
        df = df[df["_id"] >= (df["_id"].max() - pd.Timedelta(minutes=5))]

        x_min = 0
        x_max = 0
        if df["_id"].max() - df["_id"].min() < pd.Timedelta(minutes=5):
            x_min = df["_id"].min()
            x_max = x_min + pd.Timedelta(minutes=5)
        else:
            x_min = df["_id"].max() - pd.Timedelta(minutes=5)
            x_max = df["_id"].max()

        fig = px.line(
            df,
            x="_id",
            y="price",
            labels={
                "_id": "Time",
                "price": "MSFT",
            },
            range_x=[x_min, x_max],
            height=200,
        )
        fig.update_xaxes(title_text=None, showticklabels=True)
        fig.update_yaxes(title_text=None, showticklabels=True)
        fig.update_layout(
            margin=dict(l=0, r=20, t=20, b=20),
        )
        plot.plotly_chart(fig)
    elif fullyEmpty:
        df = df = df.iloc[0:0]
        last_timestamp = None
    time.sleep(1)
