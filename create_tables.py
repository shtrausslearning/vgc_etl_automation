import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import glob;
from sqlalchemy import text
from contextlib import contextmanager


def append_df_to_table(conn, df, table_name):
    """
    
    Append pandas DataFrame to existing PostgreSQL table using psycopg2
    Assumes DataFrame columns match table columns exactly
    
    """
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s (%s) VALUES %%s" % (table_name, cols)
    
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print(f"Appended {len(df)} rows to {table_name}")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


from sqlalchemy import text
from contextlib import contextmanager

def db_transaction(engine, sql_query: str, *args, **kwargs):
    """
    
    Execute SQL query in a transaction
    Rolls back on error, commits on success
    
    """
    conn = None
    try:
        conn = engine.connect()
        with conn.begin(): 
            executed = conn.execute(text(sql_query), *args, **kwargs)
            try:
                result = executed.fetchall()
            except:
                result = None
            return result
    except Exception as e:
        if conn:
            pass
        raise e
    finally:
        if conn:
            conn.close()


conn = psycopg2.connect(
    dbname="postgres"...
)

engine = create_engine('postgresql://postgres...')



def db_transaction(engine, sql_query: str, *args, **kwargs):
    """
    
    Execute SQL query in a transaction
    Rolls back on error, commits on success
    
    """
    conn = None
    try:
        conn = engine.connect()
        with conn.begin(): 
            executed = conn.execute(text(sql_query), *args, **kwargs)
            try:
                result = executed.fetchall()
            except:
                result = None
            return result
    except Exception as e:
        if conn:
            pass
        raise e
    finally:
        if conn:
            conn.close()


create_table_query = """
CREATE TABLE IF NOT EXISTS vgc.teams (
    name VARCHAR,
    tera_type VARCHAR,
    ability VARCHAR,
    item VARCHAR,
    move_1 VARCHAR,
    move_2 VARCHAR,
    move_3 VARCHAR,
    move_4 VARCHAR,
    url VARCHAR,
    event VARCHAR,
    added TIMESTAMP
)
"""

db_transaction(engine,create_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS vgc.players (
    Player_ID VARCHAR,
    First_name VARCHAR,
    Last_name VARCHAR,
    Country VARCHAR,
    Division VARCHAR,
    Trainer_name VARCHAR,
    Team_list VARCHAR,
    URL VARCHAR,
    Standing INTEGER,
    Event VARCHAR,
    Player VARCHAR,
    added TIMESTAMP
)
"""

db_transaction(engine,create_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS vgc.pairings (
    game_table VARCHAR,
    player1 VARCHAR,
    player1_record VARCHAR,
    player1_points INTEGER,
    player1_winner BOOL,
    player2 VARCHAR,
    player2_record VARCHAR,
    player2_points INTEGER,
    player2_winner BOOL,
    event VARCHAR,
    pod INT,
    round INT,
    added TIMESTAMP
);
"""

db_transaction(engine,create_table_query)
