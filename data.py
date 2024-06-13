import streamlit as st
import pandas as pd
import pytz
import datetime
from lib import init_connection

@st.cache_data(ttl=600)
def get_stock_min(selectedTickers, d1, d2, aggregation_period):
    client = init_connection()
    db = client.stocks

    pipeline = [
        {
            "$match": {
                "localDate": {"$gte": d1.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC), "$lte": d2.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)},
                "$or": [{"ticker": ticker} for ticker in selectedTickers],
            }
        },
        {
            "$sort": {"localTS": 1},
        }
    ]

    if aggregation_period == "Day":
        pipeline.append(
            {
                "$group": {
                    "_id": {"date": "$localDate", "ticker": "$ticker"},
                    "open": {"$first": "$open"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "close": {"$last": "$close"},
                    "count": {"$sum": 1},
                    "volume": {"$sum": "$volume"},
                }
            }
        )
        pipeline.append(
        {
            "$project": {
                "_id": 0,
                "ticker": "$_id.ticker",
                "date":"$_id.date",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            }
        })
    elif aggregation_period == "Hour":
        pipeline.append(
            {
                "$group": {
                    "_id": {
                        "date": {"$dateTrunc": {"date": "$localTS", "unit": "hour"}},
                        "ticker": "$ticker",
                    },
                    "open": {"$first": "$open"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "close": {"$last": "$close"},
                    "volume": {"$sum": "$volume"},
                }
            }
        ),
        pipeline.append(
        {
            "$project": {
                "_id": 0,
                "ticker": "$_id.ticker",
                "date":"$_id.date",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            }
        })
    elif aggregation_period == "Minute":
        pipeline.append(
            {
                "$group": {
                    "_id": {
                        "bucket": {"$floor":{"$divide": ["$window_start", 5 * 60 * 1000 * 1000 * 1000]}},
                        "ticker": "$ticker",
                    },
                    "date": {"$min": "$localTS"},
                    "open": {"$first": "$open"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "close": {"$last": "$close"},
                    "volume": {"$sum": "$volume"},
                }
            }
        )
        pipeline.append(
        {
            "$project": {
                "_id": 0,
                "ticker": "$_id.ticker",
                "date":1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
            }
        })

    pipeline.append({"$sort": {"date": 1}})

    df = db.stocks_min.aggregate(pipeline)
    df = pd.DataFrame(df)

    if not df.empty:
        if aggregation_period == "Day":
            df["date"] = df["date"].dt.date
        else:
            df["date"] = df["date"].dt.tz_localize("America/New_York")

    return df

@st.cache_data(ttl=600)
def get_trades_second(selectedTickers, d1, d2):
    client = init_connection()
    db = client.stocks

    pipeline = [
        {
            "$match": {
                "localDate": {"$gte": d1.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC), "$lte": d2.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)},
                "localTS": {"$gte": d1.replace(tzinfo=pytz.UTC), "$lte": d2.replace(tzinfo=pytz.UTC)},
                "$or": [{"ticker": ticker} for ticker in selectedTickers],
            }
        },
        {
            "$sort": {"localTS": 1},
        }
    ]
    pipeline.append(
        {
            "$group": {
                "_id": {
                    "date": {"$dateTrunc": {"date": "$localTS", "unit": "second"}},
                    "ticker": "$ticker",
                },
                "volume": {"$sum": "$size"},
                "open": {"$first": "$price"},
                "high": {"$max": "$price"},
                "low": {"$min": "$price"},
                "close": {"$last": "$price"},
                "count": {"$sum": 1},
            }
        }
    ),
    pipeline.append(
    {
        "$project": {
            "_id": 0,
            "ticker": "$_id.ticker",
            "date":"$_id.date",
            "volume": 1,
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "count": 1,
        }
    })
    pipeline.append({"$sort": {"date": 1}})

    df = db.trades.aggregate(pipeline)
    df = pd.DataFrame(df)

    if not df.empty:
        df["date"] = df["date"].dt.tz_localize("America/New_York")

    return df

def get_realtime_second(selectedTickers, last, realTime):
    client = init_connection()
    db = client.stocks

    match_condition = {"$or": [{"ticker": ticker} for ticker in selectedTickers]}

    if realTime:
        nytz = pytz.timezone("America/New_York")
        now = nytz.localize(datetime.datetime.now())
        match_condition["localDate"] = now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)

    if last is not None:
        match_condition["localTS"] = {"$gte": last}

    col = db.realtime if realTime else db.replay

    df = col.aggregate(
        [
            {
                "$match": match_condition
            },
            {
                "$group": {
                    "_id": {
                        "date": {"$dateTrunc": {"date": "$localTS", "unit": "second"}},
                        "ticker": "$ticker",
                    },
                    "price": {"$avg": "$price"},
                }
            },
            {
                "$project": {
                    "_id": "$_id.date",
                    "ticker": "$_id.ticker",
                    "price": 1,
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
        ]
    )

    df = pd.DataFrame(df)
    if not df.empty:
        df = {ticker: df[df["ticker"] == ticker] for ticker in selectedTickers}
    else :
        df = {ticker: pd.DataFrame() for ticker in selectedTickers}
    return df

def get_realtime_sofar(selectedTickers, realTime):
    client = init_connection()
    db = client.stocks

    match_condition = {"$or": [{"ticker": ticker} for ticker in selectedTickers]}

    if realTime:
        nytz = pytz.timezone("America/New_York")
        now = nytz.localize(datetime.datetime.now())
        match_condition["localDate"] = now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)

    col = db.realtime if realTime else db.replay

    df = col.aggregate(
        [
            {
                "$match": match_condition
            },
            {
                "$group": {
                    "_id": {
                        "bucket": {"$floor":{"$divide": ["$timestamp", 5 * 60 * 1000]}},
                        "ticker": "$ticker",
                    },
                    "date": {"$min": "$localTS"},
                    "open": {"$first": "$price"},
                    "high": {"$max": "$price"},
                    "low": {"$min": "$price"},
                    "close": {"$last": "$price"},
                    "volume": {"$sum": "$size"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "ticker": "$_id.ticker",
                    "date":1,
                    "volume": 1,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "count": 1,
                }
            },
            {
                "$sort": {
                    "date": 1
                }
            }
        ]
    )

    df = pd.DataFrame(df)
    if not df.empty:
        df = {ticker: df[df["ticker"] == ticker] for ticker in selectedTickers}
    else :
        df = {ticker: pd.DataFrame() for ticker in selectedTickers}
    return df