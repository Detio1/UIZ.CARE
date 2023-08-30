'''Load or tansfer postgresql database ( data_soucres) tables in a new postgresql 
database (uiz_care_stagging) while considering the loading frequency (daily, real-time, etc) of each  table'''

from EL import save_table
import pandas.io.sql as psql
from Postgresql_db_data_sources_connection import database_connect
import psycopg2 as pg
from sqlalchemy import create_engine

def retrieve_all_table_in_a_database(db_name):
    tables_name = []
    # Database connection details
    db_conn_string = f"postgresql://postgres:postgres@localhost/{db_name}"

    # Create a SQLAlchemy engine instance
    engine = create_engine(db_conn_string)

    # Get a list of all tables in the database
    with engine.connect() as conn:
        result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';")
        tables = [row[0] for row in result]

    for table in tables:
        tables_name.append(table)
        
    return tables_name

result = retrieve_all_table_in_a_database('data_sources')
print("List of tables in the database:", result)

connection = database_connect()

for table in result:
    df = psql.read_sql_query(f'select * from {table}', connection)
    print("table", table)
    save_table(df, table, 'uiz_care_stagging')