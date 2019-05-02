# -*- coding: utf-8 -*-
"""
Created on Fri Aug 24 14:18:53 2018

@author: mclaugha
"""
import pandas as pd
from pandasql import sqldf
import psycopg2
import math
import timeit
import sys
sys.path.append('L:/Data/Finance/Z Shared Outside Files/Analytics Team/Internal Files/CLV/')
sys.path.append('L:/data/Finance/Reporting/Report Scheduler/Python')
import pandas as pd
import HashUtility
import Utilities
database= "FPA"
username = "mclaugha"
password= "pickup28@"
schema = "marketing"
addresses_sorted= pd.read_csv("C:/Users/mclaugha/Documents/addresses_sorted.csv", header=0)
print("calculating idfs")
idfDict = HashUtility.computeAllIdf(list(addresses_sorted['cust_name_address']))
print("calculating tf idfs")
tfIdfDict = HashUtility.computeAllTfIdf(list(addresses_sorted['cust_name_address']), idfDict)

del addresses_sorted

results = dict()
 
for k in range(1, 11):
    address_h = Utilities.runPgQuery("FPA", username, password, """
                           SELECT 
  addresses.customernumber, 
  addresses.cust_name_address, 
  addresses.zip, 
  customer_to_hash_values.h"""+str(k) + """
FROM 
  marketing.addresses, 
  marketing.customer_to_hash_values
WHERE 
  addresses.customernumber = customer_to_hash_values.cust and
  customer_to_hash_values.h"""+str(k) + """ not in(
  select
  customer_to_hash_values.h"""+str(k) + """
  
  from
  marketing.customer_to_hash_values
  group by 
  customer_to_hash_values.h"""+str(k) + """
  having
  count(*)>20 
  
  )
  order by
  customer_to_hash_values.h"""+str(k) + """, 
addresses.zip """)
    
    
    print("h: " + str(k) + ", " + str(len(results))+'Time: ', timeit.default_timer())
    zips= list(address_h['zip'])
    addresses_list= list(address_h['cust_name_address'])
    cust_ids= list(address_h['customernumber'])
    
    h_codes= list(address_h['h' + str(k)])
    del address_h
    print("hash function: " + str(k))
   
    for i in range(len(h_codes)):
        curZip = zips[i]
        curHCode=h_codes[i]
        j = i + 1
        
        if (j < len((h_codes))):
            while ((h_codes[j]==curHCode) & (curZip== zips[j] ) & (tuple(sorted((cust_ids[i], cust_ids[j]))) not in results)):
                 
                score=HashUtility.softTfIdf(addresses_list[i],
                                         addresses_list[j],
                                          idfDict,
                                          tfIdfDict)
                if score>1:
                    results[tuple(sorted((cust_ids[i], cust_ids[j])))]= score
                j = j + 1
                if (j == len(h_codes)):
                    break
        
    
        #if (i%200000==0):
            #print("i: " + str(i) + ", " + str(len(results))+'Time: ', timeit.default_timer())
resultList = list()            
for result in results:
    record = list()
    record.append(result[0])
    record.append(result[1])
    record.append(results[result])
    resultList.append(record)

resultsDf = pd.DataFrame(resultList)
resultsDf.columns = ['cust1',  'cust2', 'similiarity']
Utilities.createTable(resultsDf, database, username, password, schema, "sim_scores_4", ['cust1', 'cust2'])
    

test = sqldf("""
select 
cust1, 
cust2, 
similiarity,
a1.cust_name_address as a1,
ss1,
a2.cust_name_address as a2,
ss2,
hash_function
from 
resultsDf, 
addresses_sorted as a1,  
addresses_sorted as a2 
where
 a1.CustomerNumber = cust1 and 
 a2.CustomerNumber = cust2 and
 a1.source_system = ss1 and 
 a2.source_system = ss2
 order by similiarity desc
 """)
 
test.to_csv("C:/Users/mclaugha/Documents/Python Scripts/dupe_report.csv")
test2= sqldf("""select cust1, 
cust2, 
similiarity,
ss1,
ss2 from test where similiarity>2.5 order by similiarity""")
from sqlalchemy.engine import create_engine
import urllib
conn_str = (
    r'Driver=ODBC Driver 17 for SQL Server;'
    r'Server=DWReportingPortal;'
    r'Database=Blick_DWAnalytics;'
    r'Trusted_Connection=yes;'
)
quoted_conn_str = urllib.parse.quote_plus(conn_str)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))
test2.to_sql('duplicate_customers', engine, if_exists="append", schema = "dbo")
