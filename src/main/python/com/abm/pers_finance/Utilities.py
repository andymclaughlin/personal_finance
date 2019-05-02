import pandas as pd
from pandasql import sqldf
import psycopg2
import sys


def convertListToCsvString(aList):

    out = ""

    for i in range(0, len(aList)-1):

        out=out+aList[i]+ ", "

    out = out+aList[len(aList)-1]

    return out



def runPgQuery(host, db, username, password, sql):
    conn = psycopg2.connect("dbname='" + db+"' user='" + username+"' host='"+ host+"' password='" + password+"'")
    data = pd.read_sql(sql,  conn)
    conn.close()
    return(data) 
    
def runPgUpdate(host, db, username, password, sql):
    conn = psycopg2.connect("dbname='" + db+"' user='" + username+"' host='"+ host+"' password='" + password+"'")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    


def createTable(userfolder, inputTable, host, database, username, password, schema, table, primaryKeyColumns=[], dateColumns=[], dropFlag=False):
    
    
    colNames= list()
    for x in inputTable.columns:
        colNames.append(x.lower().replace(" ", "_").replace("#", "").replace("$", "").replace("/", ""))
    
    col_types_np_type= list(inputTable.dtypes)
    col_types_string_type=list()
    pgTypeList= list()
    i=0
    for x in col_types_np_type:
        col_types_string_type.append(str(x))
        if x=="int64":
            pgTypeList.append("bigint")
        elif x=="object":
            if colNames[i] in dateColumns:
                pgTypeList.append("date")
            else:
                pgTypeList.append("text")
        elif x=="float64":
            pgTypeList.append("numeric")            
        elif x=="datetime64[ns]":
            pgTypeList.append("date")
        else:
            print("unknown type: " + str(x))
            sys.exit(1)
        i=i+1
        
   
    sql= "CREATE TABLE "+ schema + "." +table+ " ("
    for i in range(0, len(pgTypeList)):
        if i<len(pgTypeList)-1:
            sql= sql + " "+ colNames[i] + " "+ pgTypeList[i]+ ", "
    if len(primaryKeyColumns)>0:
        sql= sql+ " "+  colNames[i] + " "+ pgTypeList[i]+ ", primary key("+ convertListToCsvString(primaryKeyColumns)+ ")); " 
    else:
        sql= sql+ " "+  colNames[i] + " "+ pgTypeList[i]+ "); " 
    print(sql)
   
    tableExists = runPgQuery(host, database, username, password, """
                             SELECT EXISTS (
   SELECT 1
   FROM   information_schema.tables 
   WHERE  table_schema = '"""+ schema+"""'
   AND    table_name = '"""+ table+"""'
   );""")
    
    if tableExists['exists'][0]:
        runPgUpdate(host, database, username, password, "DELETE FROM "+ schema+ "."+table)
        if dropFlag==True:
            runPgUpdate(host, database, username, password, "drop table "+ schema+ "."+table)
            runPgUpdate(host, database, username, password, sql)
    else:        
        runPgUpdate(host, database, username, password, sql)
    inputTable.to_csv("transfer.csv", index=False, encoding='utf-8')
    
    conn = psycopg2.connect("dbname='"+database+"' user='" + username + "' host='"+ host+"' password='"+ password + "'")
    cur = conn.cursor()
    rowCountBeforeCopy = runPgQuery(host, database, username, password, "select count(*) as rowCount from " + schema + "." + table)
    with open("transfer.csv", 'r') as f:               
        cur.copy_expert("copy " + schema+"." + table+ " from STDIN CSV HEADER encoding 'utf-8' ", f)
        
    conn.commit()
   
    cur.close()
    conn.close()
    
    rowCountAfterCopy = runPgQuery(host, database, username, password, "select count(*) as rowCount from " + schema + "." + table)
    
    print("Appended " + str(rowCountAfterCopy['rowcount'][0]-rowCountBeforeCopy['rowcount'][0]) + " rows")
    
    
