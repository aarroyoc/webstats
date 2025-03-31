# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb",
#     "requests",
# ]
# ///

import argparse
import requests
import duckdb
from urllib.request import urlretrieve
from datetime import datetime, timedelta

# We take file logs-all.parquet
# If exists:
# - We download it and get the last date of the logs
# - We open the nginx log file and we append the logs after that date
# - We upload a new logs.parquet file
# If it doesn't
# - We open the nginx log file and append everything
# - We upload a new logs.parquet file

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", help="Location of log file to be ingested", required=True)
    parser.add_argument("--blob-sas-url", help="Azure Blob URL to upload Parquet file with a SAS token attached", required=True)
    args = parser.parse_args()

    url = azure_url("logs.parquet", args.blob_sas_url)
    response = requests.head(url)
    if response.status_code != 404:
        urlretrieve(url, "old-logs.parquet")
        duckdb.sql(
            """
            COPY
            (
              SELECT
                remote_addr,
                remote_user,
                time,
                status,
                body_bytes_sent,
                http_referer,
                http_user_agent,
                http_x_forwarded_for,
                regexp_extract(request, '(.+) (.+) (.+)', 1) AS method,
                regexp_extract(request, '(.+) (.+) (.+)', 2) AS path,
                regexp_extract(request, '(.+) (.+) (.+)', 3) AS protocol,
              FROM read_csv('%s', columns = {
                'remote_addr': 'VARCHAR',
                'remote_user': 'VARCHAR',
                'time': 'TIMESTAMPTZ',
                'request': 'VARCHAR',
                'status': 'SMALLINT',
                'body_bytes_sent': 'INTEGER',
                'http_referer': 'VARCHAR',
                'http_user_agent': 'VARCHAR',
                'http_x_forwarded_for': 'VARCHAR',
              })
              WHERE
                time > ( SELECT MAX(time) FROM read_parquet('old-logs.parquet'))
              UNION
              SELECT * FROM read_parquet('old-logs.parquet')
            )
            TO 'logs.parquet'
            (FORMAT 'parquet', COMPRESSION 'zstd')
            """ % args.log_file )
        requests.delete(url)
    else:
        duckdb.sql(
            """
            COPY
            (
              SELECT
                remote_addr,
                remote_user,
                time,
                status,
                body_bytes_sent,
                http_referer,
                http_user_agent,
                http_x_forwarded_for,
                regexp_extract(request, '(.+) (.+) (.+)', 1) AS method,
                regexp_extract(request, '(.+) (.+) (.+)', 2) AS path,
                regexp_extract(request, '(.+) (.+) (.+)', 3) AS protocol,
              FROM read_csv('%s', columns = {
                'remote_addr': 'VARCHAR',
                'remote_user': 'VARCHAR',
                'time': 'TIMESTAMPTZ',
                'request': 'VARCHAR',
                'status': 'SMALLINT',
                'body_bytes_sent': 'INTEGER',
                'http_referer': 'VARCHAR',
                'http_user_agent': 'VARCHAR',
                'http_x_forwarded_for': 'VARCHAR',
              })
            )
            TO 'logs.parquet'
            (FORMAT 'parquet', COMPRESSION 'zstd')
            """ % args.log_file )

    requests.put(url, headers={
        "Content-Type": "application/vnd.apache.parquet",
        "x-ms-blob-type": "BlockBlob"
    }, data=open("logs.parquet", "rb"))
        

def azure_url(filename, blob_sas_url):
    blob_split_url = blob_sas_url.split("?")
    return f"{blob_split_url[0]}/{filename}?{blob_split_url[1]}"

if __name__ == "__main__":
    main()

