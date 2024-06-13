import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

def render_stock_history(ticker, aggregation_period, df, title):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)

    d1 = min(df['date'])
    d2 = max(df['date'])

    ticker_data = df[df["ticker"] == ticker]

    full_date_range = ""
    if aggregation_period == "Day":
        full_date_range = pd.date_range(start=d1, end=d2, inclusive="left", freq="D", ambiguous=True )
    elif aggregation_period == "Hour":
        full_date_range = pd.date_range(start=d1, end=d2, inclusive="left", freq="h", ambiguous=True )
    elif aggregation_period == "Minute":
        full_date_range = pd.date_range(start=d1, end=d2, inclusive="left", freq="5min", ambiguous=True )

    dvalue = None

    if aggregation_period == "Day":
        full_date_range = full_date_range.date
    elif aggregation_period == "Hour":
        dvalue = 60 * 60 * 1000
        full_date_range = pd.DatetimeIndex(full_date_range)
    else:
        dvalue = 60 * 1000
        full_date_range = pd.DatetimeIndex(full_date_range)

    data_dates = df['date'].unique()
    missing_dates = pd.DatetimeIndex([date for date in full_date_range if date not in data_dates])

    fig.add_trace(go.Bar(
        x=ticker_data['date'],
        y=ticker_data['volume'],
        name=f'{ticker} Volume',
        yaxis='y2'
    ), row=2, col=1)

    fig.add_trace(
        go.Candlestick(
            x=ticker_data["date"],
            open=ticker_data["open"],
            high=ticker_data["high"],
            low=ticker_data["low"],
            close=ticker_data["close"],
            name=ticker,
        ), row=1, col=1
    )

    if aggregation_period == "Day":
        missing_dates = [date for date in missing_dates if date.date() >= min(ticker_data['date'])]
    else:
        missing_dates = [date for date in missing_dates if date >= min(ticker_data['date'])]

    min_date = min(ticker_data['date'])
    max_date = max(ticker_data['date'])

    ticktext = None
    if aggregation_period == "Day":
        ticktext = [min_date, max_date]
    elif aggregation_period == "Hour":
        ticktext = [min_date.strftime("%Y-%m-%d %H:%M"), max_date.strftime("%Y-%m-%d %H:%M")]
    else:
        ticktext = [min_date.strftime("%H:%M"), max_date.strftime("%H:%M")]

    fig.update_xaxes(
        tickmode='array',
        tickangle=0,
        tickvals=[min_date, max_date],
        ticktext=ticktext,
        rangebreaks=[dict(values=missing_dates, dvalue=dvalue)],
        rangeslider=dict(visible=False),
    )

    fig.update_layout(
        title=title,
        yaxis1=dict(automargin=True),
        yaxis2=dict(range=[0, max(ticker_data['volume'])], automargin=True),
        showlegend=False,
        margin=dict(l=50, r=50, t=20, b=0),
        height=200
    )

    return fig