import mysql.connector
import random
import time
import datetime

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL Database connection successful!!")
    except Exception as err:
        print(f"Error: '{err}'")
    return connection

def create_and_switch_database(connection, db_name, switch_db):
    cursor = connection.cursor()
    try:
        drop_query = "DROP DATABASE IF EXISTS "+db_name
        db_query = "CREATE DATABASE "+db_name
        switch_query = "USE "+switch_db
        cursor.execute(drop_query)
        cursor.execute(db_query)
        cursor.execute(switch_query)
        print("Database created successfully!!")
    except Exception as err:
        print(f"Error in creating database: '{err}'")


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("MySQL database connection successfully!!")
    except Exception as err:
        print(f"Error in creating connection with database: '{err}'")
    return connection

#
 # Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    cursor = connection.cursor()
    try:
        cursor.execute(table_creation_statement)
        connection.commit()
        print("Create_table query is successful!")
    except Exception as err:
        print(f"Error in insert query:'{err}'")


# retrieving the data from the table based on the given query
def select_query(connection, query):
     # fetching the data points from the table
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as err:
        print(f"Error in select query: '{err}'")

# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql,val)
        connection.commit()
        print("Query Successful")
    except Exception as err:
        print(f"Error in insert many data query: '{err}' ")