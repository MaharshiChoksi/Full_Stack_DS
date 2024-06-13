import mysql.connector as sql # from dotenv import load_dotenv
import os
from typing import Tuple
from dotenv import load_dotenv
from mysql.connector import Error

load_dotenv()

# TODO-1: MySQL connection Method
def connect_to_mysql(host:str='localhost', user:str='root', port:int=3306, database:str=None) -> Tuple[sql.connection.MySQLConnection, sql.connection.MySQLCursor]:
    """ WRITE DOCSTRING """

    connection = sql.connect(host=host, user=user, port=port, password=os.environ['PASSWORD'], database=database)

    cursor = connection.cursor()
    return connection, cursor
        
# connect_to_mysql()

# TODO-2: Database Creation Method
def create_database(conn, cursor) -> bool :
    """ WRITE DOCSTRING """
    
    try:
        query_drop_database= "DROP DATABASE IF EXISTS ds_practice;"
        query_create_database= "CREATE DATABASE ds_practice;"
        cursor.execute(query_drop_database)
        cursor.execute(query_create_database)
        conn.commit()
        print("Database Created")
        return True
    except Exception as db_create_err:
        print("Error occured while creating database: ", db_create_err)
        return False

# TODO-3: Table Creation Method
def create_table(conn, cursor, db) -> bool:
    """ WRITE DOCSTRING """
    
    try:
        query_db = f"USE {db};"
        query_drop_table = "DROP TABLE IF EXISTS students;"
        query_create_table = "CREATE TABLE students(id int NOT NULL, first_name varchar(255) NOT NULL, last_name varchar(255) NOT NULL, age int NOT NULL, phone bigint);"
        cursor.execute(query_db)
        cursor.execute(query_drop_table)
        cursor.execute(query_create_table)
        conn.commit()
        print("Table Created")
        return True
    except Exception as tb_create_err:
        print("Error occured while creating table: ", tb_create_err)
        return False


# TODO-4: Inserting Data to Table
# Another task; find what datatypes for data will be passed as argument (list, string, dictionary, tuple)
def insert_data_to_table(conn, cursor, database:str) -> bool:
    """ WRITE DOCSTRING """

    try:
        query_insert = [
        f"use {database}",
        "insert into students (id, first_name, last_name, age, phone) values (1, 'Minta', 'Wippermann', 78, 2895998422);",
        "insert into students (id, first_name, last_name, age, phone) values (2, 'Griffin', 'Furnell', 11, 8857522377);",
        "insert into students (id, first_name, last_name, age, phone) values (3, 'Tobye', 'Guillain', 26, 2207983495);",
        "insert into students (id, first_name, last_name, age, phone) values (4, 'Gal', 'Lapwood', 92, 2177056055);",
        "insert into students (id, first_name, last_name, age, phone) values (5, 'Ardra', 'Brewett', 74, 5551325933);",
        "insert into students (id, first_name, last_name, age, phone) values (6, 'Nicolis', 'Dudman', 69, 6253628898);",
        "insert into students (id, first_name, last_name, age, phone) values (7, 'Shantee', 'Birtwell', 65, 4875603253);",
        "insert into students (id, first_name, last_name, age, phone) values (8, 'Barb', 'Probet', 32, 4072367541);",
        "insert into students (id, first_name, last_name, age, phone) values (9, 'Paige', 'Critchley', 94, 6658769260);",
        "insert into students (id, first_name, last_name, age, phone) values (10, 'Chase', 'Meredith', 27, 9609026336);",
        ]
        for i in query_insert:
            cursor.execute(i)
        conn.commit()
        print("Data Inserted To table")
        return True
    except Exception as insert_error:
        print("Error occurred while inserting data to table: ", insert_error)
        return False


# TODO-5: Fetch Data Method
def get_data_from_table(table:str, cursor) -> list:
    """ Write Docstring """
    query_use_db = "USE ds_practice;"
    query_get_data = f"select * from {table};"
    
    cursor.execute(query_use_db)
    cursor.execute(query_get_data)
    return cursor.fetchall()


# TODO-6: Close Connection Method
def close_connection(curs, conn) -> bool:
    """ WRITE DOCSTRING """
    
    try:
        curs.close()
        conn.close()
        print("Connection Closed")
        return True
    except Exception as conn_error:
        print("Error while closing connection: ", conn_error)
        return False
    

def main():
    try:
        conn, curs = connect_to_mysql(host=os.environ['HOST'], user=os.environ['USER'], port=os.environ['PORT'])
        create_database(conn, curs)
        create_table(conn=conn,cursor= curs, db=os.environ['DATABASE'])
        insert_data_to_table(conn=conn, database=os.environ['DATABASE'], cursor=curs)
        returned_data = get_data_from_table(table='students', cursor=curs)
        [print (i) for i in returned_data ]
        close_connection(curs, conn)
    except Error as e:
        conn.rollback()
        print(e)


if __name__ == '__main__':
    main()
