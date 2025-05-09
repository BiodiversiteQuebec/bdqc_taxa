import psycopg2
import os

# Print current working directory
print(os.getcwd())

# Make sure to set the environment variables before running this script
# Option 1. Using vscode, edit settings.json to set the environment variables.
#   The Python extension reads the specified .env file and injects the 
#   environment variables into the Python process when it runs scripts or tests.
#     "python.envFile": "${workspaceFolder}/.env"
# }
# Option 2. Using bash before running script:
# export $(cat .env | xargs)

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'dbname': os.getenv('POSTGRES_DB')
}

def connect():
    # Raise exception if any of the values are empty
    if not all(DB_CONFIG.values()):
        raise ValueError("One or more environment variables are missing")
    
    # Create a connection with proper parameters
    conn_string = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} " \
                  f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} " \
                  f"dbname={DB_CONFIG['dbname']} application_name=atlas-db-unit-tests"
    return psycopg2.connect(conn_string)