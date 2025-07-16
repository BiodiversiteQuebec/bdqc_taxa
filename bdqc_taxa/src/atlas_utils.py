import psycopg2
import os

# Print current working directory
print(os.getcwd())

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'dbname': os.getenv('POSTGRES_DB')
}

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

def connect():
    # Raise exception if any of the values are empty
    if not all(DB_CONFIG.values()):
        raise ValueError("One or more environment variables are missing")
    
    # Create a connection with proper parameters
    conn_string = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} " \
                  f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} " \
                  f"dbname={DB_CONFIG['dbname']} application_name=atlas-db-unit-tests"
    return psycopg2.connect(conn_string)