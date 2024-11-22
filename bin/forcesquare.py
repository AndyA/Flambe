import duckdb
import polars as pl

DB = "/data/duckdb/foursquare.duckdb"

con = duckdb.connect(DB)

df = con.sql("SELECT country FROM fs").pl()
country_df = (
    df.group_by("country")
    .agg(pl.count("country").alias("count"))
    .sort("count", descending=True)
)

print(country_df)
country_df.write_json("tmp/forcesquare.json")
