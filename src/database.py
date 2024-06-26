from utils import *

"""
BluePrint: 
-> Connect To Server
-> Create Database 
-> Get Data From CSV
-> Create Schema
-> Create Table
-> insert data
-> close cursor
"""

def main():
    try:
        conn, curs = connect_to_mysql(host=os.environ['HOST'], user=os.environ['USER'], port=os.environ['PORT'])
        create_database(curs, dbname=os.environ['DATABASE'])
        df = get_data_from_csv("../data/supermarket_sales.csv")
        col_type, total_field = schema_template(df)
        create_table(cursor=curs, database=os.environ['DATABASE'], tbname=os.environ['TBNAME'], col_type=col_type)
        insert_data_to_table(conn=conn, database=os.environ['DATABASE'], cursor=curs, table=os.environ['TBNAME'], total_field=total_field, dataframe=df)
        # returned_data = get_data_from_table(table='students', cursor=curs, database=os.environ['DATABASE'])
        # [print (i) for i in returned_data ]

    except Error as error:
        conn.rollback()
        print(error)
    finally:
        curs.close()
        conn.close()
        print("Cursor & Connection Closed")


if __name__ == '__main__':
    main()
