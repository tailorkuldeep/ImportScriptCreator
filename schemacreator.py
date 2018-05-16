import re
import pandas as pd
import sys
datatypemap = {
    'test': 'test'
}

def stringContains(st, ptr):
    return re.search(ptr.upper(), st.upper()) != None
    # splitted = st.split()
    # return ptr in splitted

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
    print(query)


def main():
    
    cols = list(pd.read_csv(sys.argv[1]).columns)
    tableName = sys.argv[1][:-4]

    for col in cols:
        dataTypeMapper(col)

    createSchema(cols, tableName)


if __name__ == '__main__':
    main()
