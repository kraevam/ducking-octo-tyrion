'''
Created on Apr 24, 2013

@author: Luke
'''

class Table():
    def __init__(self, tableName, primaryKey, columns):
        self.primaryKey = primaryKey
        self.tableName = tableName
        self.dependencies = []
        self.columns = columns
        self.dependentCount = 0
        self.isNested = False
        
    def addDependency(self, dependentTable, dependentColumnName):
        if not any(dependentTable for tuple in self.dependencies if tuple[0] == dependentTable):
            if(self != dependentTable):
                dependentTable.dependentCount += 1
                self.dependencies.append((dependentTable, dependentColumnName))
        
    def addColumn(self, columnName):
        self.columns.append(columnName)
        
    def printTable(self):
        print self.tableName
        print "\tDependency Count: " + str(self.dependentCount)
        print "\tColumns:"
        for col in self.columns:
            print "\t\t" + col
        print "\tDependent Tables:"
        for dep in self.dependencies:
            print "\t\t" + dep.tableName
            
    def printScheme(self, offset, skipColumn=""):
        output = offset + "[" + self.tableName + "]\n"
        for col in self.columns:
            if col != skipColumn:
                output += offset + "\t" + col + "\n"
        for dep in self.dependencies:
            depTable = dep[0]
            depColumn = dep[1]
            output += depTable.printScheme(offset + "\t", depColumn)
        return output