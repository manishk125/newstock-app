-- Cleanup
STOP ALL PIPELINES;
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

-- BEFORE RUNNING REPLACE "ACCESSKEYID" BELOW WITH YOUR POLYGON ACCESS KEY ID
-- AND REPLACE "SECRETACCESSKEY" BELOW WITH YOUR POLYGON SECRET ACCESS KEY
CREATE DATABASE IF NOT EXISTS stocks;

DROP TABLE IF EXISTS stocks_min;

CREATE TABLE stocks_min(
  localTS AS CONVERT_TZ(FROM_UNIXTIME(window_start / 1000000000), 'UTC','America/New_York') PERSISTED DATETIME(6) NOT NULL,
  localDate AS localTS PERSISTED DATE NOT NULL,
  ticker LONGTEXT NOT NULL,
  volume BIGINT NOT NULL,
  open DOUBLE NOT NULL,
  close DOUBLE NOT NULL,
  high DOUBLE NOT NULL,
  low DOUBLE NOT NULL,
  window_start BIGINT NOT NULL,
  transactions BIGINT NOT NULL,
  INDEX (ticker),
  SORT KEY (localDate, ticker, localTS),
  SHARD KEY(ticker));

-- 2024
CREATE PIPELINE stocks_min_pipeline_2024 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2024/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2024;

-- 2023
CREATE PIPELINE stocks_min_pipeline_2023 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2023/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2023;

-- 2022
CREATE PIPELINE stocks_min_pipeline_2022 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2022/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2022;

-- 2021
CREATE PIPELINE stocks_min_pipeline_2021 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2021/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2021;

-- 2020
CREATE PIPELINE stocks_min_pipeline_2020 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2020/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2020;

-- 2019
CREATE PIPELINE stocks_min_pipeline_2019 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2019/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2019;

-- 2018
CREATE PIPELINE stocks_min_pipeline_2018 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2018/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2018;

-- 2017
CREATE PIPELINE stocks_min_pipeline_2017 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2017/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2017;

-- 2016
CREATE PIPELINE stocks_min_pipeline_2016 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2016/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2016;

-- 2015
CREATE PIPELINE stocks_min_pipeline_2015 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/minute_aggs_v1/2015/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE stocks_min
FIELDS TERMINATED BY ',' IGNORE 1 LINES;
START PIPELINE stocks_min_pipeline_2015;

DROP TABLE IF EXISTS trades;
CREATE TABLE trades(
  localTS AS CONVERT_TZ(FROM_UNIXTIME(sip_timestamp / 1000000000), 'UTC','America/New_York') PERSISTED DATETIME(6) NOT NULL,
  localDate AS localTS PERSISTED DATE NOT NULL,
  ticker LONGTEXT NOT NULL,
  conditions LONGTEXT NOT NULL,
  correction BIGINT NOT NULL,
  exchange BIGINT NOT NULL,
  id BIGINT NOT NULL,
  participant_timestamp BIGINT NOT NULL,
  price DOUBLE NOT NULL,
  sequence_number BIGINT NOT NULL,
  sip_timestamp BIGINT NOT NULL,
  size BIGINT NOT NULL,
  tape BIGINT NOT NULL,
  trf_id BIGINT NOT NULL,
  trf_timestamp BIGINT NOT NULL,
  INDEX (ticker),
  SORT KEY (localDate, ticker, localTS),
  SHARD KEY(ticker));

CREATE PIPELINE trades_pipeline_2024 AS
LOAD DATA S3 's3://flatfiles/us_stocks_sip/trades_v1/2024/*/*.csv.gz'
CONFIG '{"region":"us-east-1", "endpoint_url": "https://files.polygon.io"}'
CREDENTIALS '{"aws_access_key_id": "ACCESS_KEY_ID",
               "aws_secret_access_key": "SECRET_ACCESS_KEY"}'
INTO TABLE trades
FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES;
START PIPELINE trades_pipeline_2024;
