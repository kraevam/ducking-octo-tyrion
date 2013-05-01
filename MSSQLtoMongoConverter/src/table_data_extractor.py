'''
Created on May 1, 2013

@author: kraevam
'''
import json

class TableDataExtractor():
    '''
    Connects to the SQL DB, extracts data and saves it as JSON objects
    '''
        
    def extractData(self, cursor, table, columnName="", key=""):
        selectStatement = "SELECT * FROM " + table.tableName
        if key != "":
            selectStatement += " WHERE " + columnName + "=" + str(key)
        cursor.execute(selectStatement)
        rows = cursor.fetchall()
        primaryColumn = table.primaryKey[0] #it's a tuple
        primaryColIdx = 0;
        result = []
        for row in rows:
            jsonData = {}
            for i in range(0, len(row.cursor_description)):
                t = row.cursor_description[i]
                if t[0] != columnName:
                    jsonData[t[0]] = str(row[i]).strip()
                
                if primaryColumn == t[0]:
                    primaryColIdx = i

            for dependency in table.dependencies:
                depKey = row[primaryColIdx]
                depData = self.extractData(cursor, dependency[0], dependency[1], depKey)      
                jsonData[dependency[0].tableName] = depData
            
            result.append(jsonData)
        
        return result