import pyodbc
#import pymongo
from pymongo import MongoClient

from table import Table
from table_data_extractor import TableDataExtractor

SERVER_ADDRESS = 'whale.csse.rose-hulman.edu'
DATABASE_NAME = 'ducking-octo-tyrion'
DRIVER = '{SQL Server Native Client 10.0}'
UID = 'kraevam'
PASSWORD = 'kraevam'

MONGO_DATABASE_ADDRESS = "mongodb.csse.rose-hulman.edu"

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

def buildDependencyGraph(tableNames, columnData, pKeys, fKeys):
    tables = []
    for table in tableNames:
        columns = [row.COLUMN_NAME for row in columnData if row.TABLE_NAME == table]
        thisTable = Table(table, pKeys[table], columns)
        tables.append(thisTable)
    for key in fKeys:
        foreignTable = [table for table in tables if table.tableName == key.FK_TABLE_NAME][0]
        foreignKey = key.FK_COLUMN_NAME
        masterTable = [table for table in tables if table.tableName == key.REFERENCED_TABLE_NAME][0]
        masterTable.addDependency(foreignTable, foreignKey)
    return [table for table in tables if table.dependentCount == 0]
        
def delta(graph, cursor, mongoDB):
    extractor = TableDataExtractor()
    for table in graph:
        sqlKeys = extractor.getPrimaryKeyValues(cursor, table)
        #print "SQL Keys: " + str(sqlKeys)
        mongoKeys = [str(entry[table.primaryKey[0]]) for entry in mongoDB[table.tableName].find()]
        #print "Mongo Keys: " + str(mongoKeys)
        
        sqlSet = set(sqlKeys)
        mongoSet = set(mongoKeys)
        newKeys = sqlSet.difference(mongoSet)
        insertNewKeys(mongoDB, cursor, extractor, newKeys, table, table.primaryKey[0])
        deletedKeys = mongoSet.difference(sqlSet)
        deleteRemovedKeys(mongoDB, deletedKeys, table.tableName, table.primaryKey[0])
        
def insertNewKeys(db, cursor, tableExtractor, keys, collection, keyName):
    for key in keys:
        print "Inserting new key " + key + " into " + collection.tableName
        documents = tableExtractor.extractData(cursor, collection, keyName, key, False)
        db[collection.tableName].insert(documents)
        
def deleteRemovedKeys(db, keys, collectionName, keyName):
    for key in keys:
        removeParam = {keyName:key}
        db[collectionName].remove(removeParam)
        print "Removed ID " + key + " from " + collectionName
        
def main():
    connectionString = 'driver=' + DRIVER + ';server=' + SERVER_ADDRESS + ';database=' + DATABASE_NAME + ';uid=' + UID + ';pwd=' + PASSWORD
    cnxn = pyodbc.connect(connectionString)
    cursor = cnxn.cursor()
    mongoClient = MongoClient(MONGO_DATABASE_ADDRESS)
    db = mongoClient[DATABASE_NAME]
    
    columnData = getColumnData(cursor)
    tableNames = set(row.TABLE_NAME for row in columnData)
    pKeys = getPrimaryKeys(cursor)
    fKeys = getForeignKeys(cursor)
    graph = buildDependencyGraph(tableNames, columnData, pKeys, fKeys)
    
    if len(db.collection_names()) < 1:
        print "Exporting data"        
        tablesJSON = {}
        for table in graph: 
            tableDataExtractor = TableDataExtractor()
            result = tableDataExtractor.extractData(cursor, table)
            tablesJSON[table.tableName] = result

        for tableName in tablesJSON:
            for entry in tablesJSON[tableName]:
                db[tableName].insert(entry)
    else:
        print "Checking for changes."
        delta(graph, cursor, db)
    print "Done"
main()
