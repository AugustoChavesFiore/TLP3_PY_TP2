import sys
import MySQLdb

def connect_DB():
    try:
        db = MySQLdb.connect('localhost','root','','tp_csv_db')
        print('DB Connected')
    except MySQLdb.Error as e:
        print ('Error DB : %s' % e)
        sys.exit(1)
    return db