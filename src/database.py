from utils import (connect_to_mysql, create_database,
                       get_data_from_csv,schema_template,
                       create_table, insert_data_to_table)
import os
from mysql.connector import Error
import argparse


"""
BluePrint: 
-> parse arguments
-> Connect To Server
-> Create Database 
-> Get Data From CSV
-> Create Schema
-> Create Table
-> insert data
-> close cursor
"""

parser = argparse.ArgumentParser(prog='database',
                                 description='Connects to database, reads the file and insert data to database')

parser.add_argument('-dbc', '--db_create',
                    default='F',
                    type=str,
                    help='pass True to create new database or else False')

parser.add_argument('-f', '--filename',
                    type=str,
                    help='pass file name of the csv file')

parser.add_argument('-tn', '--tb_name',
                    type=str,
                    help='pass the name of the table')

parser.add_argument('-dn', '--db_name',
                    type=str,
                    required=True,
                    help='pass the name of the database')


args = parser.parse_args()


conn, curs = connect_to_mysql(host=os.environ['HOST'], user=os.environ['USER'], port=os.environ['PORT'])

if args.db_create in ['T', 'TI']:
    create_database(curs, dbname=args.db_name)
if args.db_create in ['F', 'TI']:
    df = get_data_from_csv(args.filename)
    col_type, total_field = schema_template(df)
    create_table(cursor=curs, database=args.db_name, tbname=args.tb_name, col_type=col_type)
    insert_data_to_table(conn=conn, database=args.db_name, cursor=curs, table=args.tb_name, total_field=total_field, dataframe=df)
    # returned_data = get_data_from_table(table='students', cursor=curs, database=args.db_name)
    # [print (i) for i in returned_data ]

curs.close()
conn.close()
print("Cursor & Connection Closed")
