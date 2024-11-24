import streamlit as st
import polars as pl
from datetime import datetime, UTC

st.title("WebStats")

website = st.sidebar.selectbox("Website", ("blog", "files", "ppt", "prologhub", "social"))
today = datetime.now(UTC)
first_day_month = datetime(today.year, today.month, 1)
dates = st.sidebar.date_input("Date range", (first_day_month, today))

urls = {
    "blog": "https://adrianistanlogs.blob.core.windows.net/blog-adrianistan-eu/logs.parquet?sp=r&st=2024-10-20T14:41:26Z&se=2025-10-20T22:41:26Z&spr=https&sv=2022-11-02&sr=c&sig=Nl50RZu4RhprAx%2F0RD%2F1IakEh89jtAiJhNsRBAod7yE%3D",
    "files": "https://adrianistanlogs.blob.core.windows.net/files-adrianistan-eu/logs.parquet?sp=r&st=2024-10-20T15:42:14Z&se=2025-10-20T23:42:14Z&spr=https&sv=2022-11-02&sr=c&sig=ov2oGR6A1bXAbjpOOhUUfPZNVv1aShUeJ5U46%2FuZU40%3D",
    "ppt": "https://adrianistanlogs.blob.core.windows.net/ppt-adrianistan-eu/logs.parquet?sp=r&st=2024-10-20T15:46:17Z&se=2025-10-20T23:46:17Z&spr=https&sv=2022-11-02&sr=c&sig=%2FLzAuBx1ugfbScoGHtc3m4ddLACfkG6lRVNndPyZPog%3D",
    "prologhub": "https://adrianistanlogs.blob.core.windows.net/prologhub-com/logs.parquet?sp=r&st=2024-10-20T15:49:38Z&se=2025-10-20T23:49:38Z&spr=https&sv=2022-11-02&sr=c&sig=KPosXJYfLwln%2FCt0BaIJshbbURRS90QxlJp%2Bpq%2Bj0fo%3D",
    "social": "https://adrianistanlogs.blob.core.windows.net/social-adrianistan-eu/logs.parquet?sp=r&st=2024-10-20T15:52:48Z&se=2025-10-20T23:52:48Z&spr=https&sv=2022-11-02&sr=c&sig=sBCORc%2FvSvSOHGOI%2BFynAUzgxlxGNqsvWDr7%2B7LfWmA%3D"}

hosts = {
    "blog": "blog.adrianistan.eu",
    "files": "files.adrianistan.eu",
    "ppt": "ppt.adrianistan.eu",
    "prologhub": "prologhub.com",
    "social": "social.adrianistan.eu"}

@st.cache_data
def download_data(url):
    return pl.read_parquet(url)

host = hosts[website]
df = download_data(urls[website])
df = df.filter(pl.col("time").dt.replace_time_zone(None).is_between(dates[0], dates[1]))
st.write(df)

st.header("Popular pages")
st.write(df.group_by(pl.col("path")).agg(pl.len().alias("count")).sort("count", descending=True))
# st.bar_chart(df.group_by(pl.col("path")).agg(pl.len().alias("count")).sort("count", descending=True), x="path", y="count")

st.header("Data sent in MB")
bdf = df.group_by(pl.col("path")) \
    .agg((pl.sum("body_bytes_sent") / 1000000).alias("megabytes")) \
    .sort("megabytes", descending=True)

st.write(bdf)
st.write(bdf.select(pl.col("megabytes").sum()))

st.header("User Agents")
st.write(
    df.group_by(pl.col("http_user_agent"))
    .agg(pl.len().alias("count"))
    .sort("count", descending=True))

st.header("Referrals")
st.subheader("External")
st.write(
    df.filter(pl.col("http_referer").str.contains(f"^https?://{host}") == False)
    .group_by(pl.col("http_referer"))
    .agg(pl.len().alias("count"))
    .sort("count", descending=True))
st.subheader("Internal")
st.write(
    df.filter(pl.col("http_referer").str.contains(f"^https?://{host}") == True)
    .group_by(pl.col("http_referer"))
    .agg(pl.len().alias("count"))
    .sort("count", descending=True))

st.header("Broken pages")
st.write(
    df.filter(pl.col("status") == 404)
    .group_by(pl.col("path"))
    .agg(pl.len().alias("count"))
    .sort("count", descending=True))

