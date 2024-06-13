# Real-Time Stocks Demo

This demo explores how SingleStore can power real-time analytics applications such as stock trading.
It highlights some key concepts:

1. SingleStore pipelines can ingest terabytes of data in an automated fashion and on an ongoing
   basis from sources such as S3.

2. SingleStore supports low-latency analytical queries over the ingested data.

3. SingleStore accepts high-volume real-time writes and immediately supports real-time queries over
   the fresh data.

4. Data in SingleStore can be read and written through either a SQL interface or a
   MongoDB-compatible interface.

## Demo Premise

The idea of this demo is a stock-trading dashboard.

```sql
STOP PIPELINE stocks_min_pipeline_2024;
STOP PIPELINE stocks_min_pipeline_2023;
STOP PIPELINE stocks_min_pipeline_2022;
STOP PIPELINE stocks_min_pipeline_2021;
STOP PIPELINE stocks_min_pipeline_2020;
STOP PIPELINE stocks_min_pipeline_2019;
STOP PIPELINE stocks_min_pipeline_2018;
STOP PIPELINE stocks_min_pipeline_2017;
STOP PIPELINE stocks_min_pipeline_2016;
STOP PIPELINE stocks_min_pipeline_2015;

STOP PIPELINE trades_pipeline_2024;
STOP PIPELINE trades_pipeline_2023;

DROP PIPELINE stocks_min_pipeline_2024;
DROP PIPELINE stocks_min_pipeline_2023;
DROP PIPELINE stocks_min_pipeline_2022;
DROP PIPELINE stocks_min_pipeline_2021;
DROP PIPELINE stocks_min_pipeline_2020;
DROP PIPELINE stocks_min_pipeline_2019;
DROP PIPELINE stocks_min_pipeline_2018;
DROP PIPELINE stocks_min_pipeline_2017;
DROP PIPELINE stocks_min_pipeline_2016;
DROP PIPELINE stocks_min_pipeline_2015;

DROP PIPELINE trades_pipeline_2024;
DROP PIPELINE trades_pipeline_2023;

```