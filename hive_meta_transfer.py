#!/usr/bin/python
import time
from pyhive import hive

source_hive =  'hive1' # the source hive 
destination_hive = 'hive2'  # the destination hive
string_fix =  'namenode:8020' #  the namenode on the source hive cluster


def create_databases():

    cursor = hive.connect(source_hive).cursor()
    cursor.execute('SHOW DATABASES')
    time.sleep(1)
    dba = cursor.fetchall()
    cursor.close()
    decoded_dba = [[word.decode("utf8") for word in sets] for sets in dba]
    for dbas in decoded_dba:  # fix unicode
        encoded_dbas = u' '.join(dbas)
        print "DBName: %s" % encoded_dbas
        try:
            cursor = hive.connect(source_hive).cursor()
            cursor.execute('USE %s' % encoded_dbas)
            cursor.execute('SHOW TABLES')
            cursor.close()
            cursor = hive.connect(destination_hive).cursor()
            cursor.execute('CREATE DATABASE %s' % encoded_dbas)
            cursor.close()
        except:
            pass


def create_table(database_name, table_name):

    # connect to the production cluster
    cursor = hive.connect(source_hive).cursor()
    cursor.execute("SHOW CREATE TABLE %s.%s" % (database_name, table_name))
    time.sleep(1)
    df = cursor.fetchall()
    cursor.close()  # we close the handle
    decoded = [[word.decode("utf8") for word in sets] for sets in df]
    query = []
    for words in decoded:  # fix unicode
        result = u' '.join(words)
        query.append(result)
    query_str = ''.join(query)
    query_str = query_str.replace(string_fix,'')
    cursor = hive.connect(destination_hive).cursor()  # connect to the tef cluster
    print "creating %s.%s" %  (database_name, table_name)
    cursor.execute(query_str)
    time.sleep(5)
    cursor.execute("msck repair table %s.%s" % (database_name, table_name))  # repair the table
    time.sleep(5)
    cursor.close()  # close the second handle


def iterate_tables(database_name):
    cursor = hive.connect(source_hive).cursor()
    cursor.execute("USE %s" % database_name)
    cursor.execute("SHOW TABLES")
    df = cursor.fetchall()
    decoded = [[word.decode("utf8") for word in sets] for sets in df]
    cursor.close()
    query = []
    for words in decoded:  # fix unicode
        result = u' '.join(words)
        query.append(result)
    for i in query:
        try:
            create_table(database_name, i)
        except:
            pass


def find_source_dbs():
    cursor = hive.connect(source_hive).cursor()
    cursor.execute('SHOW DATABASES')
    time.sleep(1)
    dba = cursor.fetchall()
    cursor.close()
    decoded_dba = [[word.decode("utf8") for word in sets] for sets in dba]
    query = []
    for words in decoded_dba:  # fix unicode
        result = u' '.join(words)
        query.append(result)
    return query


def main():
    create_databases()

    for i in find_source_dbs():
        iterate_tables(i)


if __name__ == "__main__":
    main()
