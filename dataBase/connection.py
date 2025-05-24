import psycopg2
import os

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    try:
        # Don't set environment variables for now
        # os.environ['PGCLIENTENCODING'] = 'UTF8'

        # Connect to the database with explicit encoding parameters
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database="DW-PROJECT",
            user="postgres",
            password="yassir123",
            # Try a different encoding that might handle the problematic characters
            options="-c client_encoding=LATIN1",
            client_encoding='utf8'
        )
        
        # Don't explicitly set encoding after connection
        # connection.set_client_encoding('UTF8')
        
        print(f"Client encoding: {connection.get_parameter_status('client_encoding')}")
        print(f"Server encoding: {connection.get_parameter_status('server_encoding')}")
        
        return connection

    except psycopg2.Error as error:
        print(f"Error connecting to PostgreSQL: {error}")
        raise