import sqlite3
from sqlite3 import Error
import json
import csv
import os


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def select_task_by_priority(conn, priority):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE priority=?", (priority,))

    rows = cur.fetchall()

    for row in rows:
        print(row)


def main():
    database = r"/Users/murlishenoy/Documents/Finance/sandbox.db"

    # # create a database connection
    # conn = create_connection(database)
    # with conn:
    #     print("1. Query task by priority:")
    #     select_task_by_priority(conn, 1)

    #     print("2. Query all tasks")
    #     select_all_tasks(conn)


    try:
        
        # Making a connection between sqlite3
        # database and Python Program
        sqliteConnection = sqlite3.connect(database)
        
        # If sqlite3 makes a connection with python
        # program then it will print "Connected to SQLite"
        # Otherwise it will show errors
        print("Connected to SQLite")

        # Getting all tables from sqlite_master
        sql_query = """SELECT name FROM sqlite_master
        WHERE type='table';"""

        # Creating cursor object using connection object
        cursor = sqliteConnection.cursor()
        
        # executing our sql query
        cursor.execute(sql_query)
        print("List of tables\n")
        
        # printing all tables list
        #print(cursor.fetchall())
        tables_list = cursor.fetchall();
        print (tables_list)

        #table = cursor.execute("SELECT * FROM transactions")

        sqlite_select_query = """SELECT * FROM transactions"""
        cursor.execute(sqlite_select_query)
        records = cursor.fetchall()
        print("Total rows are:  ", len(records))
        print("Printing each row")
        array = []
        for row in records:
            print("account_id ", row[0])
            print("transaction_id ", row[1])
            print("created ", row[2])
            print("updated ", row[3])
            print("plaid_json: ", row[5])
            print("\n")
            my_dict = json.loads(row[5])
            items_to_remove = ["location","payment_meta","category"]
            for key in items_to_remove:
                del my_dict[key]

            array.append(my_dict)
            #print (my_dict.keys())
            
            
        keys = array[0].keys()
        with open('transactions.csv', 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(array)

        
        cursor.close()
        print ("Start array")
        print (array)
        print ("end array")

        #SELECT column1, column2, columnN FROM table_name
    except sqlite3.Error as error:
        print("Failed to execute the above query", error)
        
    finally:

        # Inside Finally Block, If connection is
        # open, we need to close it
        if sqliteConnection:
            
            # using close() method, we will close
            # the connection
            sqliteConnection.close()
            
            # After closing connection object, we
            # will print "the sqlite connection is
            # closed"
            print("the sqlite connection is closed")

if __name__ == '__main__':
    main()