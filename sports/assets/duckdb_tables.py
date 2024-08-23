# from dagster import asset
# import requests, pandas as pd, os, duckdb


# def show_table():
#     conn = duckdb.connect(database="./sports/duckdb/raw.duckdb")

#     try:
#         query_result = conn.execute("SELECT * FROM raw.teams.rosters").df()

#         print(query_result)
#         return query_result
#     finally:
#         return conn.close()


# print(show_table())

