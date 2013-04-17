'''
Created on Apr 17, 2013

@author: kraevam
'''
import sys
sys.path.append("C:\Python27\DLLs")
import pyodbc

SERVER_ADDRESS = 'whale.csse.rose-hulman.edu'
DATABASE_NAME = 'ducking-octo-tyrion'
DRIVER = '{SQL Server Native Client 10.0}'
UID = 'kraevam'
PASSWORD = 'kraevam'

PRIMARY_KEY_SELECT_STATEMENT = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1"
FOREIGN_KEY_SELECT_STATEMENT = "SELECT   KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME ,KCU1.TABLE_NAME AS FK_TABLE_NAME ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME   FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC     LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME     LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION"
COLUMN_DATA_SELECT = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS"


def getColumnData(cursor):
    cursor.execute(COLUMN_DATA_SELECT)
    rows = cursor.fetchall()
    return rows


def getForeignKeys(cursor):
    cursor.execute(FOREIGN_KEY_SELECT_STATEMENT)
    rows = cursor.fetchall()
    return rows

def getPrimaryKeys(cursor):
    cursor.execute(PRIMARY_KEY_SELECT_STATEMENT)
    rows = cursor.fetchall()
    result = {}
    for row in rows:
        # keep keys in tuples, in case we have a composite primary key
        if not row.TABLE_NAME in result:
            result[row.TABLE_NAME] = (row.COLUMN_NAME,);
        else:
            result[row.TABLE_NAME] = result[row.TABLE_NAME] + (row.COLUMN_NAME,)
        
    return result

def main():
    connectionString = 'driver=' + DRIVER + ';server=' + SERVER_ADDRESS + ';database=' + DATABASE_NAME + ';uid=' + UID + ';pwd=' + PASSWORD
    cnxn = pyodbc.connect(connectionString)
    cursor = cnxn.cursor()
    
    columnData = getColumnData(cursor)
    tableNames = set(row.TABLE_NAME for row in columnData)
    
    pKeys = getPrimaryKeys(cursor)
    fKeys = getForeignKeys(cursor)
    
    for table in tableNames:
        print 'Table name: ' + table
        print 'Table columns: ',
        print [row.COLUMN_NAME for row in columnData if row.TABLE_NAME == table]
        print 'Primary key: ', pKeys[table]
        print 
        
    print 'Foreign keys:'
    for fKey in fKeys:
        print fKey
                
main()
