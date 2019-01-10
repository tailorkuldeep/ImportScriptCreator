import re
import pandas as pd
import sys
import os
#import configparser
import glob

# config = configparser.ConfigParser()
# config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.ini'))

datatypemap = {
    'test': 'test'
}

def getBulkQuery(filepath,tablename):    
    query  = """
BULK INSERT [dbo].["""+tablename+"""]
FROM '"""+filepath+"""' 
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR= ',',
    ROWTERMINATOR = '\\n'
);
"""
    return query


def stringContains(st, ptr):
    return re.search(ptr.upper(), st.upper()) != None

def dataTypeMapper(colname):
    backwards_name = str([x for x in reversed(colname.split())])
    knownKeyWords = {
        'COST': 'FLOAT',
        '%': 'FLOAT',
        'COUNT': 'INT',
        'DATE': 'NVARCHAR(30)',
        'FLAG' : 'INT'
    }
    DEFAULT = 'NVARCHAR(500)'
    for keyword in knownKeyWords.keys():
        if(stringContains(backwards_name, keyword)):
            datatypemap[colname] = knownKeyWords[keyword.upper()]
            break
        else:
            datatypemap[colname] = DEFAULT


def addCol(col):
    return "\t["+col+"] "+datatypemap[col]+",\n"


def createSchema(cols, tableName):
    query = "CREATE TABLE [" + tableName + "](\n"
    for col in cols:
        query += addCol(col)
    query = query[:-2]
    query += "\n);"
    return query

def createSchemaFor(csv_name):
    cols = list(pd.read_csv(csv_name,nrows = 1).columns)
    tableName = csv_name[:-4]
    for col in cols:
        dataTypeMapper(col)
    return createSchema(cols, tableName)

def main():
    names = glob.glob("*.csv")    
    for name in names:
        schema = createSchemaFor(name)
        bulk_query = getBulkQuery(os.path.join(os.path.abspath(os.path.dirname(__file__)), name),name[:-4])
        print(schema)
        print(bulk_query)
        f = open(name[:-4]+".sql",'w')
        f.write(schema)
        f.write(bulk_query)
        f.close()
if __name__ == '__main__':
    main()
