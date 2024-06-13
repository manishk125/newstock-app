import streamlit as st
import pandas as pd
import datetime
import plotly.express as px

from lib import init_nav, mermaid, init_connection
from chart import render_stock_history
from data import get_stock_min, get_trades_second

title = "Demo Architecture"

st.set_page_config(
    page_title=title,
    layout="centered",
)

init_nav()

st.markdown(
    f"""
    # {title}
    The demo is supported by six main components.
    1. [Streamlit Demo App](#streamlit-demo-app)
    2. [SingleStore Cluster](#singlestore-cluster)
        1. [Sizing](#sizing)
        2. [Table Structure](#table-structure) ([Historical Data](#historical-data), [Reference Data](#reference-data), [Real-Time Data](#real-time-data))
        3. [Pipelines](#pipelines)
    3. [Market Data Provider](#market-data-provider)
    4. [Real-Time Client](#real-time-client)
    5. [Replay Client](#replay-client)
    6. [Periodic Client](#periodic-client)

    These are organized as in the following diagram:
    """
)
mermaid(
    """
    %%{init: {'theme':'neutral'}}%%
    flowchart LR
        polygon:::polygon2

        browser(Browser) --> streamlit
        streamlit(Streamlit Demo App) --> database[("SingleStore Cluster")]
        subgraph polygon ["Market Data Provider"]
            s3(Bulk S3 API)
            rest(REST API)
            websocket(Real-Time WebSocket API)
        end
        subgraph snowflake ["Snowpark Container Services"]
            streamlit
            database
            rtc
            rpc
            pc
        end
        database ~~~ rtc
        database ~~~ pc
        database ~~~ rpc
        database --> s3
        rtc --> database
        rtc(Real-Time Client) --> websocket
        pc(Periodic Client) --> rest
        pc(Periodic Client) --> database
        rpc(Replay Client) --> database
        classDef subgraphStyle fill:none,stroke-dasharray:5 5,rx:8,ry:8,font-weight:bold
        class polygon subgraphStyle
        class polygon labelText
        class snowflake subgraphStyle
        class snowflake labelText
    """,
    250,
)

st.markdown(
    """
    ### Streamlit Demo App
    This demo app is built with [Streamlit](https://streamlit.io/), a Python library for building interactive web applications. It uses the PyMongo driver to get data.
    ```python
    @st.cache_resource
    def init_connection():
        return pymongo.MongoClient(st.secrets["singlestore_kai_uri"])
    ```
    The 'singlestore_kai_uri' here points to the SingleStore Cluster's SingleStore Kai endpoint. This is a MongoDB-compatible endpoint providing access to all data within a SingleStore cluster, regardless of whether it is organized into a traditional table or an unstructured MongoDB collection. For example, this Streamlit code retrieves data from a regular SQL table, and through SingleStore Kai it comes back as a BSON document.
    ```python
    client = init_connection()
    db = client.stocks
    df = db.stocks_min.find_one({"ticker":"SNOW"})
    df
    ```
    """
)
client = init_connection()
db = client.stocks
df = db.stocks_min.find_one({"ticker": "SNOW"})
df
st.markdown(
    """
    Aggregations are described using the MongoDB aggregation pipeline syntax and are charted with Plotly. For example, here's some code to chart the volume for SNOW on April 9th, 2024 by hour, including all extended hours.
    ```python
    df = db.stocks_min.aggregate(
    [
        {
            "$match": {
                "ticker": "SNOW",
                "localDate": {"$eq": datetime.datetime(2024, 4, 9)},
            }
        },
        {
            "$group": {
                "_id": {"$dateTrunc": {"date": "$localTS", "unit": "hour"}},
                "volume": {"$sum": "$volume"},
            }
        },
    ])
    df = pd.DataFrame(df)
    fig = px.bar(df, x="_id", y="volume", labels={"_id": "Hour", "volume": "Volume"})
    st.plotly_chart(fig)
    ```
    """
)
client = init_connection()
db = client.stocks
df = db.stocks_min.aggregate(
    [
        {
            "$match": {
                "ticker": "SNOW",
                "localDate": {"$eq": datetime.datetime(2024, 4, 9)},
            }
        },
        {
            "$group": {
                "_id": {"$dateTrunc": {"date": "$localTS", "unit": "hour"}},
                "volume": {"$sum": "$volume"},
            }
        },
    ]
)
df = pd.DataFrame(df)
fig = px.bar(df, x="_id", y="volume", labels={"_id": "Hour", "volume": "Volume"})
st.plotly_chart(fig)
st.markdown(
    """
    That's about all the Streamlit Demo App does — query the database using the PyMongo driver. The more interesting stuff happens in the SingleStore Cluster.
    """
)

st.markdown(
    """
    ### SingleStore Cluster
    A SingleStore Cluster is a distributed multi-modal database. Data is organized into tables and can be queries using either SQL or through SingleStore Kai, a MongoDB-compatible API. For a full overview of SingleStore, read more [here](https://www.singlestore.com/).

    #### Sizing
    The SingleStore Cluster used in this demo is equivalent to an S-2 instance size in the SingleStore Cloud. The two leaf nodes each have:
    - 8 vCPU
    - 128 GiB of memory
    - 1 TiB of disk cache
    
    SingleStore's architecture can store any amount of data in object storage, but queries are accelerated for the set of data that fits in the disk cache.

    This isn't a very large cluster, but it's powerful enough to serve the purposes of this demo thanks to the efficiency of SingleStore's columnar storage format and execution engine.

    #### Table Structure
    The SingleStore Cluster will hold three categories of data:

    1. Historical stock minute aggregates and trades
    2. Reference data (company information, etc.)
    3. Real-time trades from the current trading session

    Each type will be stored in a separate set of tables.

    ##### Historical Data

    The Market Data Provider provides historical data through a bulk S3 interface in compressed CSV format, both minute-aggregate and unaggregated data on trades. This CSV data is a natural fit for regular SQL tables, one for each record type. Here's the definition for the minute aggregates table.

    ```sql
    CREATE TABLE `stocks_min` (
        `localTS` as CONVERT_TZ(FROM_UNIXTIME(window_start / 1000000000), 'UTC','America/New_York') PERSISTED datetime(6) NOT NULL,
        `localDate` as localTS PERSISTED date NOT NULL,
        `ticker` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
        `volume` bigint(20) NOT NULL,
        `open` double NOT NULL,
        `close` double NOT NULL,
        `high` double NOT NULL,
        `low` double NOT NULL,
        `window_start` bigint(20) NOT NULL,
        `transactions` bigint(20) NOT NULL,
        KEY `localDate_2` (`localDate`,`ticker`) USING HASH,
        SORT KEY `localDate` (`localDate`,`ticker`,`localTS`),
        SHARD KEY `__SHARDKEY` (`ticker`)
    )
    ```

    The shard key determines how data is distributed across the cluster. Queries that constrain the shard key can often be routed to a single leaf node, rather than fan out across many leaf nodes. While it's not such a big deal for this demo cluster with only two leaf nodes, it's important for larger clusters. Here the queries will almost always constrain the ticker symbol in some way, so we use `SHARD KEY (ticker)`.
    
    The sort key determines the order in which data is stored on disk. This is important in SingleStore for two reasons. First, it's more efficient to read data stored together. Second, the order of the data can have a big impact on columnar compression, which uses techniques such as RLE and integer delta encoding. Randomness present in a random ordering cannot be compressed, so if the data is naturally-ordered in some way, it's best to capture that in the sort key. With that in mind, here we use `SORT KEY (localDate, ticker , localTS)`.

    1. `localDate` — we will usually query data with a time constraint. Putting a day-granularity first in the sort key allows SingleStore to perform significant segment elimination based on the days requested.
    2. `ticker` — we will usually query data with a ticker constraint. Putting ticker in the sort key will help locality of reads and compression of the ticker-dependent columns like price data.
    3. `localTS` — putting the fine-grained timestamp last enables further efficient filtering on time ranges and helps with compression of columns that vary slowly across time.

    > Why not put localTS first? Not all tickers have data at each fine-grained timestamp value (this is especially important for the trades table which is not pre-aggregated at any level). Putting localTS first would prevent runs of data from the same ticker from being stored together.

    This table also has a secondary index on (localDate, ticker) which allows further refinement of what data will be read during a query.

    The `trades` table has a similar structure.

    ```sql
    CREATE TABLE `trades` (
        `localTS` as CONVERT_TZ(FROM_UNIXTIME(sip_timestamp / 1000000000), 'UTC','America/New_York') PERSISTED datetime(6) NOT NULL,
        `localDate` as localTS PERSISTED date NOT NULL,
        `ticker` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
        `conditions` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
        `correction` bigint(20) NOT NULL,
        `exchange` bigint(20) NOT NULL,
        `id` bigint(20) NOT NULL,
        `participant_timestamp` bigint(20) NOT NULL,
        `price` double NOT NULL,
        `sequence_number` bigint(20) NOT NULL,
        `sip_timestamp` bigint(20) NOT NULL,
        `size` bigint(20) NOT NULL,
        `tape` bigint(20) NOT NULL,
        `trf_id` bigint(20) NOT NULL,
        `trf_timestamp` bigint(20) NOT NULL,
        KEY `localDate_2` (`localDate`,`ticker`) USING HASH,
        SORT KEY `localDate` (`localDate`,`ticker`,`localTS`),
        SHARD KEY `__SHARDKEY` (`ticker`)
    )
    ```

    ##### Reference Data

    Reference data comes in JSON format and will be queried by ticker, so we don't need to worry about the schema and can just write the API response from the Market Data Provider through SingleStore Kai.

    ```javascript
    db.reference.findOne({"_id":"SNOW"})
    ```
    ```json
    { _id: 'SNOW',
        details: 
        { ticker: 'SNOW',
            name: 'Snowflake Inc.',
            market: 'stocks',
            locale: 'us',
            primary_exchange: 'XNYS',
            type: 'CS',
            active: true,
            currency_name: 'usd',
            cik: '0001640147',
            composite_figi: 'BBG007DHGNJ4',
            share_class_figi: 'BBG007DHGNK2',
            market_cap: 45625988783.32,
            phone_number: '844-766-9355',
            address: 
            { address1: '106 EAST BABCOCK STREET',
                address2: 'SUITE 3A',
                city: 'BOZEMAN',
                state: 'MT',
                postal_code: '59715' },
            description: 'Founded in 2012, Snowflake is a data lake, warehousing, and sharing company that came public in 2020. To date, the company has over 3,000 customers, including nearly 30% of the Fortune 500 as its customers. Snowflake\'s data lake stores unstructured and semistructured data that can then be used in analytics to create insights stored in its data warehouse. Snowflake\'s data sharing capability allows enterprises to easily buy and ingest data almost instantaneously compared with a traditionally months-long process. Overall, the company is known for the fact that all of its data solutions that can be hosted on various public clouds.',
            sic_code: '7372',
            sic_description: 'SERVICES-PREPACKAGED SOFTWARE',
            ticker_root: 'SNOW',
            homepage_url: 'https://www.snowflake.com',
            total_employees: 7004,
            list_date: '2020-09-16',
            branding: 
            { logo_url: 'https://api.polygon.io/v1/reference/company-branding/c25vd2ZsYWtlLmNvbQ/images/2024-06-01_logo.svg',
                icon_url: 'https://api.polygon.io/v1/reference/company-branding/c25vd2ZsYWtlLmNvbQ/images/2024-06-01_icon.png' },
            share_class_shares_outstanding: 335040000,
            weighted_shares_outstanding: 335041774,
            round_lot: 100 } }
    ```

    When the table is created implicitly by inserting data through SingleStore Kai, by default it will have the following simple structure.
    ```sql
    CREATE TABLE `foo` (
        `_id` bson NOT NULL,
        `_more` bson NOT NULL COMMENT 'KAI_MORE' ,
        `$_id` as BSON_NORMALIZE_NO_ARRAY(`_id`) PERSISTED longblob COMMENT 'KAI_AUTO' ,
        SHARD KEY `__SHARDKEY` (`$_id`),
        UNIQUE KEY `__PRIMARY` (`$_id`) USING HASH,
        SORT KEY `__UNORDERED` ()
    )
    ```

    ##### Real-Time Data

    The real-time data comes through a websocket interface in the following compact format.

    ```json
    {
        "ev": "T",
        "sym": "FRGT",
        "i": "66",
        "x": 11,
        "p": 0.6689,
        "s": 133,
        "c": [14,12,41],
        "t": 1716586954057,
        "q": 6001331,
        "z": 3
    }
    ```

    The fields we care about are sym, p, s, t, and q.
    
    | Field | Description |
    | --- | --- |
    | sym | ticker |
    | p | price |
    | s | size |
    | t | timestamp (unix milliseconds) |
    | q | sequence number (unique per ticker increasing number) |

    <br />
    <p>
    Unlike the S3 data, the socket feed timestamp is unix millisecond, not nanosecond, so the sequence number is used to correctly order trades beyond localTS.
    </p>
    
    ```sql
    CREATE TABLE `realtime` (
        `localTS` as CONVERT_TZ(FROM_UNIXTIME(timestamp / 1000), 'UTC','America/New_York') PERSISTED datetime(6) NOT NULL,
        `localDate` as localTS PERSISTED date NOT NULL,
        `ticker` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
        `price` double NOT NULL,
        `sequence_number` bigint(20) NOT NULL,
        `timestamp` bigint(20) NOT NULL,
        `size` bigint(20) NOT NULL,
        KEY `localDate_2` (`localDate`,`ticker`) USING HASH,
        SORT KEY `localDate` (`localDate`,`ticker`,`localTS`,`sequence_number`),
        SHARD KEY `__SHARDKEY` (`ticker`)
    )
    ```

    For demo purposes, our "replay" table will be identical to this one, but will be populated by replaying the historical trade data for the first ten minutes of trading on April 9th, 2024. This will work exactly like the actual realtime table, with a modified realtime client doing the ingestion, but will make the demo interesting outside of market hours.

    #### Pipelines

    Now that the tables are defined, let's get some data! SingleStore offers a feature called pipelines for pulling data from many sources. We can use pipelines to import data from the Market Data Provider's bulk S3 interface in compressed CSV format with this SQL command.

    ```sql
    CREATE PIPELINE stocks_min_pipeline_2024 AS
        LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2024/*/*.csv.gz'
        CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
        CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
                    "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
        INTO TABLE stocks_min
    FIELDS TERMINATED BY ',' IGNORE 1 LINES;

    START PIPELINE stocks_min_pipeline_2024;
    ```

    Once the pipeline is set up, it will automatically pull all files from that source, and keep checking for new ones. It's a simple way to keep the data synchronized. Here's the last 5 pipeline batches that ran:
    """,unsafe_allow_html=True
)
client = init_connection()
db = client.information_schema
df = db.command({"sql":"SELECT pipeline_name, batch_state, start_time, rows_streamed FROM pipelines_batches_summary ORDER BY start_time DESC LIMIT 5"})
df = pd.DataFrame(df["cursor"]["firstBatch"])
df

st.markdown(
    """
    For the demo, pipelines are set up for all minute aggregates since 2015 and all trades data in 2024. So far the pipelines have brought in about 3 billion and 8 billion rows respectively.
    """
)
client = init_connection()
db = client.information_schema
df = db.command({"sql":"SELECT table_name, SUM(rows):>DOUBLE as rows FROM information_schema.table_statistics WHERE database_name = 'stocks' AND partition_type = 'master' GROUP BY table_name"})
df = pd.DataFrame(df["cursor"]["firstBatch"])
df

st.markdown(
    """
    ### Market Data Provider
    The market data for this demo comes from [polygon.io](https://polygon.io/). Polygon.io provides multiple data sources.

    1. [An S3 API](https://polygon.io/flat-files) for bulk historical data.
    2. [A Web Socket API](https://polygon.io/docs/stocks/ws_getting-started) for real-time data
    3. [A REST API](https://polygon.io/docs/stocks/getting-started) for on-demand data.
    
    These provide everything needed for a real-time stocks analytics system.
    """
)

st.markdown(
    """
    ### Real-Time Client
    The real-time client pulls from Polygon's Web Socket API and writes to the realtime table in the database via SingleStore Kai. What message rate must it handle? Here is the per-second trading volume for April 9th.
    """
)

db = client.stocks
df = db.trades.aggregate(
    [
        {
            "$match": {
                "localDate": {"$eq": datetime.datetime(2024, 4, 9)},
                "localTS": {
                    "$gte": datetime.datetime(2024, 4, 9, 9, 30),
                    "$lte": datetime.datetime(2024, 4, 9, 16, 00)},
            }
        },
        {
            "$group": {
                "_id": {"$dateTrunc": {"date": "$localTS", "unit": "minute"}},
                "volume": {"$sum": 1},
            }
        },
        {"$addFields": {"volume": {"$divide": ["$volume", 60]}}},
        {"$sort": {"_id": 1}}
    ]
)
df = pd.DataFrame(df)
fig = px.line(df, x="_id", y="volume", labels={"_id": "Time", "volume": "Trades per Second"})
st.plotly_chart(fig)

st.markdown(
    """
    From this day, the real-time client needs to handle a peak approaching 30,000 trades per second. However, the short message size should keep it to just 3 MB/s, so as long as the real-time client is efficiently written it should be able to easily handle the load as a single instance without requiring multiple clients sharded by ticker or some other scheme.

    For this demo, the real-time client is a short Go application written to convert the incoming JSON messages from the web socket directly to BSON without constructing any object model.

    ### Replay Client
    The same Go program for the real-time client is adapted to also run in 'replay' mode, where it reads from the historical trades table and writes into the replay table, resetting every ten minutes by truncating the replay table. This client runs at the same data rate as the real-time client.
    """
)

st.markdown(
    """
    ### Periodic Client
    To round out the system architecture, the periodic client runs once-per-day to get company information from the Market Data Provider. This program reads the REST API of the Market Data Provider and writes the full JSON response into the database.
    """
)