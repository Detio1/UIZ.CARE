import psycopg2

# Database connection
def database_connect():

  db_params = {
    "dbname": "data_sources",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}
  connection = psycopg2.connect(**db_params)
    
  return connection
