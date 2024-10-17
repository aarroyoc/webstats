import argparse
import requests
import polars as pl
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", help="Location of log file to be ingested", required=True)
    parser.add_argument("--blob-sas-url", help="Azure Blob URL to upload Parquet file with a SAS token attached", required=True)
    args = parser.parse_args()

    today = datetime.now()

    # We check for the last 7 days, if the file exists on Azure
    for i in range(7):
        date = today - timedelta(days=i+1)
        if not exists_log_file_in_cloud(args.blob_sas_url, date):
            upload_file_cloud(args.blob_sas_url, args.log_file, date)

def log_file(date):
    return date.strftime("logs-%Y-%m-%d.parquet")

def azure_url(blob_sas_url, date):
    date_str = log_file(date)
    blob_split_url = blob_sas_url.split("?")
    return f"{blob_split_url[0]}/{date_str}?{blob_split_url[1]}"

def exists_log_file_in_cloud(blob_url, date):
    response = requests.head(azure_url(blob_url, date))
    return response.status_code != 404

def upload_file_cloud(blob_url, log_file, date):
    url = azure_url(blob_url, date)

    pl.scan_csv(
        log_file,
        try_parse_dates=True,
        has_header=False,
        new_columns=["remote_addr", "remote_user", "time", "request",
                     "status", "body_bytes_sent", "http_referer",
                     "http_user_agent", "http_x_forwarded_for"],
    ).filter(
        pl.col("time").dt.year() == date.year,
        pl.col("time").dt.month() == date.month,
        pl.col("time").dt.day() == date.day,
    ).with_columns(
        method=pl.col("request").str.extract("(.+) (.+) (.+)", 1),
        path=pl.col("request").str.extract("(.+) (.+) (.+)", 2),
        protocol=pl.col("request").str.extract("(.+) (.+) (.+)", 3),
    ).collect().write_parquet("logs.parquet", compression="zstd")

    requests.put(url, headers={
        "Content-Type": "application/vnd.apache.parquet",
        "x-ms-blob-type": "BlockBlob"
    }, data=open("logs.parquet", "rb"))

if __name__ == "__main__":
    main()
