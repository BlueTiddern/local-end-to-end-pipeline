import pandas as pd
import yaml
import os, sys
from pathlib import Path
from sqlmodel import create_engine, Session
from sqlalchemy import text
import numpy as np

def load_yml(path : str) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def make_dir(path: Path | str):
    os.makedirs(path, exist_ok=True)

# create the engine and session
def mysql_connect_create_db(db_name : str, db_user : str,host : str,port : str, password : str, create_flag : bool = True):

    if not password:
        raise ValueError("DB_PASSWORD missing from .env")

    conn_string = f"mysql+pymysql://{db_user}:{password}@{host}:{port}"

    engine = create_engine(conn_string,echo=False, future=True)

    if create_flag:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            conn.commit()


def get_engine_session(db_name : str,db_user : str,host : str,port : str, password : str) -> tuple :

    engine = create_engine(
        f"mysql+pymysql://{db_user}:{password}@{host}:{port}/{db_name}",
        echo=False,
        future=True
    )

    session = Session(engine)

    return engine,session

def recreate_table(engine, model) -> None:
    table = model.__table__
    try:
        table.drop(engine, checkfirst=True)
        #print(f"Dropped table {table.name}")
    except Exception as e:
        print(f"Warning: failed to drop table {table.name}: {e}")

    try:
        table.create(engine)
        #print(f"Created table {table.name}")
    except Exception as e:
        print(f"Error creating table {table.name}: {e}")

def gmd_null_handler(df_group_object) -> pd.DataFrame:
    """
    This utility is to take the group by object from Global macro data
    Fill the Null values with median or with 0 if all the year values are missing
    """

    #copy of the pandas group by object
    group_object = df_group_object.sort_values('year').copy()

    for col in ['NOMINAL_GDP','REAL_GDP','INFLATION','UNEMPLOYMENT']:
        series = group_object[col]
        if series.notna().sum() == 0:
            group_object[col + '_FILLED'] = pd.Series(0.0, index = series.index)
            continue
        series_median = series.median(skipna=True)
        group_object[col + '_FILLED'] = series.fillna(series_median)

    return group_object




