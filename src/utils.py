import mysql.connector as sql # from dotenv import load_dotenv
import os
from typing import Tuple
from dotenv import load_dotenv, find_dotenv
from mysql.connector import Error

load_dotenv(override=True)

# TODO-1: MySQL connection Method
def connect_to_mysql(host:str='localhost', user:str='root', port:int=3306, database:str=None) -> Tuple[sql.connection.MySQLConnection, sql.connection.MySQLCursor]:
    """ Connection to MySQL Server

    Args:
        host (str, optional): Server Address. Defaults to 'localhost'.
        user (str, optional): Username. Defaults to 'root'.
        port (int, optional): port Number. Defaults to 3306.
        database (str, optional): Database Name to connect at login. Defaults to None.

    Returns:
        Tuple[sql.connection.MySQLConnection, sql.connection.MySQLCursor]: returns sql connection string and cursor in form of tuple
    """

    try:
        connection = sql.connect(host=host, user=user, port=port, password=os.environ['PASSWORD'], database=database)

        cursor = connection.cursor()
        return connection, cursor
    except Error as conn_err:
        raise(conn_err)


# TODO-2: Database Creation Method
def create_database(conn:sql.connection.MySQLConnection, cursor:sql.connection.MySQLCursor) -> bool :
    """ Drop database if exists and create new one

    Args:
        conn (sql.connection.MySQLConnection):  connection string as argument
        cursor (sql.connection.MySQLCursor):  cursor string as argument

    Returns:
        bool: returns True/False if query executed successfully or not
    """
    
    try:
        query_drop_database= "DROP DATABASE IF EXISTS ds_practice;"
        query_create_database= "CREATE DATABASE ds_practice;"
        cursor.execute(query_drop_database)
        cursor.execute(query_create_database)
        conn.commit()
        print("Database Created")
        return True
    except Error as db_create_err:
        raise(db_create_err)


# TODO-3: Table Creation Method
def create_table(conn:sql.connection.MySQLConnection, cursor:sql.connection.MySQLCursor, database:str) -> bool:
    """ drops table if exists and create new one with provided parameters for columns 

    Args:
        conn (sql.connection.MySQLConnection:  connection string as argument
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name to insert table 

    Returns:
        bool: returns True/False if query executed successfully or not
    """
    
    try:
        query_db = f"USE {database};"
        query_drop_table = "DROP TABLE IF EXISTS students;"
        query_create_table = "CREATE TABLE students(id int NOT NULL, first_name varchar(255) NOT NULL, last_name varchar(255) NOT NULL, age int NOT NULL, phone bigint);"
        cursor.execute(query_db)
        cursor.execute(query_drop_table)
        cursor.execute(query_create_table)
        conn.commit()
        print("Table Created")
        return True
    except Error as tb_create_err:
        raise(tb_create_err)


# TODO-4: Inserting Data to Table
# Another task; find what datatypes for data will be passed as argument (list, string, dictionary, tuple)
def insert_data_to_table(conn:sql.connection.MySQLConnection, cursor:sql.connection.MySQLCursor, database:str, table:str) -> bool:
    """Insert data to given table from the database provided as an argument

    Args:
        conn (sql.connection.MySQLConnection:  connection string as argument
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name to insert table 
        table (str): table name to which the data will be inserted

    Returns:
        bool: returns True/False if query executed successfully or not
    """

    try:
        query_insert = [
        f"use {database}",
        f"insert into {table} (id, first_name, last_name, age, phone) values (1, 'Minta', 'Wippermann', 78, 2895998422);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (2, 'Griffin', 'Furnell', 11, 8857522377);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (3, 'Tobye', 'Guillain', 26, 2207983495);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (4, 'Gal', 'Lapwood', 92, 2177056055);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (5, 'Ardra', 'Brewett', 74, 5551325933);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (6, 'Nicolis', 'Dudman', 69, 6253628898);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (7, 'Shantee', 'Birtwell', 65, 4875603253);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (8, 'Barb', 'Probet', 32, 4072367541);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (9, 'Paige', 'Critchley', 94, 6658769260);",
        f"insert into {table} (id, first_name, last_name, age, phone) values (10, 'Chase', 'Meredith', 27, 9609026336);",]
        
        for i in query_insert:
            cursor.execute(i)
        conn.commit()
        print("Data Inserted To table")
        return True
    except Error as insert_error:
        raise(insert_error)


# TODO-5: Fetch Data Method
def get_data_from_table(table:str, cursor:sql.connection.MySQLCursor, database:str) -> list:
    """ Pull data from the table in the database passed in argument

    Args:
        table (str):  table name from where the data will be fetched
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name where table is located

    Returns:
        list: returns retrived data in form of list
    """
    try:
        query_use_db = f"USE {database};"
        query_get_data = f"select * from {table};"
        
        cursor.execute(query_use_db)
        cursor.execute(query_get_data)
        return cursor.fetchall()
    except Error as pull_data_error:
        raise(pull_data_error)
        


# TODO-6: Close Connection Method
def close_connection(curs:sql.connection.MySQLCursor, conn:sql.connection.MySQLConnection) -> bool:
    """ Closes Connection To Server

    Args:
        curs (sql.connection.MySQLCursor): Cursor string
        conn (sql.connection.MySQLConnection): Connection string

    Returns:
        bool: returns True if connection is closed else False
    """
    
    try:
        curs.close()
        conn.close()
        print("Connection Closed")
        return True
    except Error as conn_error:
        raise(conn_error)



""" optimize this code to return direct error without crashing code in this case
A). when connection to sserver failed then what to do as exception will also throw error while rollback
"""
def main():
    try:
        conn, curs = connect_to_mysql(host=os.environ['HOST'], user=os.environ['USER'], port=os.environ['PORT'])
        create_database(conn, curs)
        create_table(conn=conn,cursor= curs, database=os.environ['DATABASE'])
        insert_data_to_table(conn=conn, database=os.environ['DATABASE'], cursor=curs, table="students")
        returned_data = get_data_from_table(table='students', cursor=curs, database=os.environ['DATABASE'])
        [print (i) for i in returned_data ]

    except Error as error:
        conn.rollback()
        print(error)
    finally:
        close_connection(curs=curs, conn=conn)


if __name__ == '__main__':
    main()
