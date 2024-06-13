import streamlit as st
import pandas as pd
import datetime
import pytz
import time
import plotly.express as px

from lib import get_tickers, init_nav, init_connection
from data import get_stock_min, get_realtime_second, get_realtime_sofar
from chart import render_stock_history

st.set_page_config(
    page_title="Real-Time Stocks With SingleStore",
    layout="wide",
)

init_nav()

tickers = get_tickers()

st.title("Dashboard")

selectedTickers = st.multiselect(
    "Tickers", tickers, ["INTC", "NVDA", "MSFT", "SNOW"]
)

if "selectedTickers" not in st.session_state:
    st.session_state.selectedTickers = selectedTickers
elif st.session_state.selectedTickers != selectedTickers:
    print("rerun")
    st.session_state.selectedTickers = selectedTickers
    st.rerun()

rt = st.radio("Real-time or replay?", ["Real-Time", "Replay"], index=0)

nytz = pytz.timezone("America/New_York")
now = datetime.datetime.now(nytz)

df = pd.DataFrame(columns=["ticker"])

st.write("Updated as of " + now.strftime("%A, %B %d, %Y %H:%M:%S" + " EST"))

client = init_connection()
db = client.stocks

plots = [None] * len(selectedTickers)
plots2 = [None] * len(selectedTickers)
cols = st.columns(len(selectedTickers))
for index, selectedTicker in enumerate(selectedTickers):
    cols[index].header(selectedTicker)
    # get the name from the reference table using the mongo client
    ref = db.reference.find_one({"_id": selectedTicker})
    cols[index].write(ref["details"]["name"])
    plots[index] = cols[index].empty()
    plots2[index] = cols[index].empty()

# Previous Trading Day

d2 = now.replace(hour=0, minute=0, second=0, microsecond=0)
while df.empty:
    d2 -= datetime.timedelta(days=1)
    while d2.weekday() == 0:
        d2 -= datetime.timedelta(days=1)
    d2 = d2.replace(hour=0, minute=0, second=0, microsecond=0)
    dd2 = nytz.localize(datetime.datetime(d2.year, d2.month, d2.day))
    dd1 = dd2 + datetime.timedelta(days=-1)
    df = get_stock_min(selectedTickers, dd1, dd2, "Minute")
df = df[
    (df["date"].dt.time >= datetime.time(9, 30))
    & (df["date"].dt.time < datetime.time(16, 0))
]
cutoff_time = df["date"].max() - datetime.timedelta(days=1)
df = df[df["date"] > cutoff_time]

a = [None] * len(selectedTickers)
for index, selectedTicker in enumerate(selectedTickers):
    a[index] = cols[index].empty()

for index, selectedTicker in enumerate(selectedTickers):
    if df.empty:
        plots[index].write("No data available")
    dff = df[df["ticker"] == selectedTicker]
    fig = render_stock_history(selectedTicker, "Minute", dff, "Previous Trading Day" if index == 0 else "")
    a[index].plotly_chart(fig)

# Last 3 Weeks

# d2 = now
# d2 = (d2 + datetime.timedelta(days=1)).replace(
#     hour=0, minute=0, second=0, microsecond=0
# )
# dd2 = nytz.localize(datetime.datetime(d2.year, d2.month, d2.day))
# dd1 = dd2 + datetime.timedelta(weeks=-3)
# df = get_stock_min(selectedTickers, dd1, dd2, "Hour")
# df = df[
#     (df["date"].dt.time >= datetime.time(9, 00))
#     & (df["date"].dt.time < datetime.time(16, 0))
# ]

# a = [None] * len(selectedTickers)
# for index, selectedTicker in enumerate(selectedTickers):
#     a[index] = cols[index].empty()

# for index, selectedTicker in enumerate(selectedTickers):
#     if df.empty:
#         plots[index].write("No data available")
#     dff = df[df["ticker"] == selectedTicker]
#     fig = render_stock_history(selectedTicker, "Hour", dff, "Trading in Previous 3 Weeks" if index == 0 else "")
#     a[index].plotly_chart(fig)

# Last 90 Days

d2 = now
d2 = (d2 + datetime.timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
dd2 = nytz.localize(datetime.datetime(d2.year, d2.month, d2.day))
dd1 = dd2 + datetime.timedelta(days=-90)
df = get_stock_min(selectedTickers, dd1, dd2, "Day")

a = [None] * len(selectedTickers)
for index, selectedTicker in enumerate(selectedTickers):
    a[index] = cols[index].empty()

for index, selectedTicker in enumerate(selectedTickers):
    if df.empty:
        plots[index].write("No data available")
    dff = df[df["ticker"] == selectedTicker]
    fig = render_stock_history(selectedTicker, "Day", dff, "Trading In Previous 90 Days" if index == 0 else "")
    a[index].plotly_chart(fig)

last_timestamp = None

dfs = [None] * len(selectedTickers)
for index, selectedTicker in enumerate(selectedTickers):
    dfs[index] = pd.DataFrame(columns=["_id", "price"])

initialized = [False] * len(selectedTickers)

emptyCount = 0
while True:
    #if rt == "Real-Time":
    #    sofar_dfs = get_realtime_sofar(selectedTickers, True)
    #    for index, selectedTicker in enumerate(selectedTickers):
    #        sofar_df = sofar_dfs[selectedTicker]
    #        fig = render_stock_history(selectedTicker, "Minute", sofar_df, "Day So Far" if index == 0 else "")
    #        plots2[index].plotly_chart(fig)

    new_dfs = get_realtime_second(selectedTickers, last_timestamp, rt == "Real-Time")
    fullyEmpty = all([new_df.empty for new_df in new_dfs.values()])
    max_ids = [new_df["_id"].max() for new_df in new_dfs.values() if not new_df.empty]
    new_last_timestamp = max(max_ids) if max_ids else last_timestamp
    written = False

    for index, selectedTicker in enumerate(selectedTickers):
        new_df = new_dfs[selectedTicker]

        if len(new_df) > 0:
            new_df = new_df[:-1]

        if last_timestamp == None or new_last_timestamp > dfs[index]["_id"].max():
            written = True

            if not new_df.empty:
                if dfs[index].empty:
                    dfs[index] = new_df
                else:
                    dfs[index] = pd.concat([dfs[index], new_df], ignore_index=True)
                    dfs[index].drop_duplicates(subset="_id", keep="last", inplace=True)
                dfs[index] = dfs[index][
                    dfs[index]["_id"]
                    >= (dfs[index]["_id"].max() - pd.Timedelta(minutes=2))
                ]

            x_min = 0
            x_max = 0

            x_min = new_last_timestamp - pd.Timedelta(minutes=2)
            x_max = new_last_timestamp

            if not dfs[index].empty:
                fig = px.line(
                    dfs[index],
                    x="_id",
                    y="price",
                    labels={
                        "_id": "Time",
                        "price": selectedTicker,
                    },
                    range_x=[x_min, x_max],
                    height=200,
                )
                fig.update_xaxes(
                    title_text=None,
                    showticklabels=True,
                    type="date",
                    range=[x_min, x_max],
                )
                fig.update_yaxes(title_text=None, showticklabels=True)
                fig.update_layout(
                    title="Last Five Minutes" if index == 0 else "",
                    margin=dict(l=0, r=20, t=20, b=20),
                )
                plots[index].plotly_chart(fig)
    if fullyEmpty and rt != "Real-Time":
        print("resetting replay")
        dfs = [None] * len(selectedTickers)
        last_timestamp = None
    elif written:
        last_timestamp = new_last_timestamp
    time.sleep(2)
