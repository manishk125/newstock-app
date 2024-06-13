import streamlit as st
import pandas as pd
import pymongo
import streamlit.components.v1 as components

# Render a mermaid diagram
def mermaid(code: str, height:int) -> None:
    components.html(
        f"""

        <div style="display:block;margin-bottom:100px">
            <style>
                .labelText span {{
                    color: #000 !important;
                }}
                @media (prefers-color-scheme: dark) {{
                .labelText span {{
                    color: #000 !important;
                    background-color: #fff !important;
                }}
                }}
            </style>
            <pre class="mermaid">
                {code}
            </pre>
        </div>

        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
        </script>
        """,
        height=height,
    )

# This application uses the MongoDB client for interacting with SingleStore.
# SingleStore also supports MySQL client drivers.
@st.cache_resource
def init_connection():
    print("init_connection()")
    return pymongo.MongoClient(st.secrets["singlestore_kai_uri"])

# Refresh the list of possible stock tickers each day
@st.cache_data(ttl="1d",show_spinner=False)
def get_tickers():
    print("get_tickers()")
    client = init_connection()
    db=client.stocks
    pipeline = [{"$group": {"_id": "$ticker"}}, {"$sort": {"_id": 1}}]
    tickersCursor = db.stocks_min.aggregate(pipeline)
    return pd.DataFrame(tickersCursor)

# Common navigation
def init_nav():
    st.sidebar.title("Navigation")
    st.sidebar.page_link("app.py", label="🙋 Welcome")
    st.sidebar.page_link("pages/demoarchitecture.py", label="🏗️ Demo Architecture")
    st.sidebar.page_link("pages/dashboard.py", label="📈 Dashboard")
    st.sidebar.page_link("https://www.singlestore.com", label="🔗 Learn More @ SingleStore.com")

# Warm the cache of tickers and the database connection (called at bottom of initial page)
def warm_cache():
    tickers = get_tickers()
