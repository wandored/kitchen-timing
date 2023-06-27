import pandas as pd
import numpy as np
import psycopg2
from config import Config
from timing_report import run
from psycopg2 import sql
from psycopg2.errors import IntegrityError, UniqueViolation
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Time
from sqlalchemy.dialects.postgresql import insert

pd.options.mode.chained_assignment = None
  
def send_to_database():
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    conn = psycopg2.connect(
        host=Config.HOST_SERVER,
        database=Config.PSYCOPG2_DATABASE,
        user=Config.PSYCOPG2_USER,
        password=Config.PSYCOPG2_PASS,
    )
    
    # Check to see if the table "timings" exists
    cur = conn.cursor()
            
    # Call timing report to get the data
    df = run()
    
    if df.empty:
        return
    table_name = "timings"
    temp_table_name = "temp_table"
    try:
        df.to_sql(temp_table_name, engine, if_exists="replace", index=False)
        upsert_query = sql.SQL("""
       INSERT INTO {table} (date, export_number, bar, dining_room, handheld, patio, online_ordering)
       SELECT t.date::date, t.export_number, t.bar::time, t.dining_room::time, t.handheld::time, t.patio::time, t.online_ordering::time
        FROM {temp_table} AS t
        ON CONFLICT (date, export_number) DO UPDATE
        SET bar = EXCLUDED.bar,
            dining_room = EXCLUDED.dining_room,
            handheld = EXCLUDED.handheld,   
            patio = EXCLUDED.patio,
            online_ordering = EXCLUDED.online_ordering
       """).format(table=sql.Identifier(table_name), temp_table=sql.Identifier(temp_table_name))
        cur.execute(upsert_query)
        conn.commit()
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        # drop temporary table
        cur.execute("DROP TABLE IF EXISTS {} CASCADE".format(temp_table_name))
        conn.commit()

    return 0
    
    return

if __name__ == "__main__":
    send_to_database()
