#!/usr/bin/env python
# coding: utf-8
import argparse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

parser = argparse.ArgumentParser(description='Ingest a table into PostgreSql docker container.')
parser.add_argument('--pg-user', type=str, default='root', help='PostgreSQL user')
parser.add_argument('--pg-pass', type=str, default='root', help='PostgreSQL password')
parser.add_argument('--pg-host', type=str, default='localhost', help='PostgreSQL host')
parser.add_argument('--pg-port', type=str, default='ny_taxi', help='PostgreSQL port')
parser.add_argument('--pg-db', type=str, default='newyork_taxi_data', help='PostgreSQL database name')
parser.add_argument('--target-table', type=str, default='newyork_taxi_data', help='PostgreSQL database name')
parser.add_argument('--url', type=str, default='', help='URL of the table')
parser.add_argument('--filetype', type=str, default='csv', help='type of the table')
parser.add_argument('--chunksize', type=int, default='100000', help='Chunk size for reading table')
args = parser.parse_args()

def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
        file_type: str = "csv",
) -> pd.DataFrame:
    if file_type == "csv":
        df_iter = pd.read_csv(
            url,
            iterator=True,
            chunksize=chunksize
        )
        first_chunk = next(df_iter)

        first_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )

        print(f"Table {target_table} created")

        first_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print(f"Inserted first chunk: {len(first_chunk)}")

        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append"
            )
            print(f"Inserted chunk: {len(df_chunk)}")
    elif file_type == "parquet":
        df = pd.read_parquet(url)
        chunks = np.array_split(df, len(df) // chunksize + 1)
        for chunk in tqdm(chunks):
            chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",   # "append", "fail"
                index=False,
                chunksize=chunksize
            )
    else:
        raise ValueError(f"The input file format {file_type} is not supported!")
    print(f'done ingesting to {target_table}')

def main():
    engine = create_engine(f'postgresql://{args.pg_user}:{args.pg_pass}@{args.pg_host}:{args.pg_port}/{args.pg_db}')

    ingest_data(
        url=args.url,
        engine=engine,
        target_table=args.target_table,
        chunksize=args.chunksize,
        file_type=args.filetype,
    )

if __name__ == '__main__':
    main()