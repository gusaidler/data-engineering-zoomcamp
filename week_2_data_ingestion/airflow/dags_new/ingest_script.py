#!/usr/bin/env python
# coding: utf-8

import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, parquet_file):

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print('Connection to Postgres established')

    df = pd.read_parquet(parquet_file)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    print(f'Data inserted to table {table_name}')