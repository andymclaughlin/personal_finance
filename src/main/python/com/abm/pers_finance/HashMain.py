import pandas as pd
import timeit
import HashUtility
import Utilities
import regex as re

regex = re.compile('[^a-zA-Z]')

database= "postgres"
username = "postgres"
password= "docker"
schema = "public"
host = "localhost"
userfolder = ""



tran_descriptions = list(Utilities.runPgQuery(host, 
    database, username, password, 
    "select distinct description from public.merged_trans")['description'])


print("calculating idfs")
idfDict = HashUtility.computeAllIdf(tran_descriptions)
print("calculating tf idfs")
tfIdfDict = HashUtility.computeAllTfIdf(tran_descriptions, idfDict)


print("computing hash values.") 
hf = HashUtility.HashFunc(15485863, 32452843)


hashValues= list()
i=0
for d in tran_descriptions:
    hashValues.append(list())
    d2= d.split()
    for token in d2:
        for j in range(1,11):
            if(len(hashValues[i])<j):
                hashValues[i].append(float('inf'))                
            hashValues[i][j-1]= min(hf.hash(token, j),hashValues[i][j-1])          
    hashValues[i].append(d)
    i=i+1

    
hdf= pd.DataFrame(hashValues)
hdf.columns=["h1","h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "descrip"]

Utilities.createTable(userfolder, hdf, host, database, username, password, schema, "descrip_to_hash_values", primaryKeyColumns=["descrip"])


del tran_descriptions



print("calculating similarity scores")
results = dict()
 
for k in range(1, 11):
    d_to_h= Utilities.runPgQuery(host, database, username, password, """
                           SELECT 
descrip,
h"""+str(k)+""" as h
FROM 
  public.descrip_to_hash_values d
  
  
  order by
2, 1 """)
    
    
    print("h: " + str(k) + ", results: "+ str(len(results))+', time: ', timeit.default_timer())
    descrips= list(d_to_h['descrip'])
    h_codes= list(d_to_h['h'])
    
    print("hash function: " + str(k))
   
    for i in range(len(h_codes)):
        j = i + 1
        curHCode= h_codes[i] 
        if (j < len((h_codes))):
            while (h_codes[j]==curHCode):
                 
                score=HashUtility.softTfIdf(descrips[i],
                                         descrips[j],
                                          idfDict,
                                          tfIdfDict)
                
                results[tuple((descrips[i], descrips[j]))]= score
                results[tuple((descrips[j], descrips[i]))]= score
                j = j + 1
                if (j == len(h_codes)):
                    break
        
    
resultList = list()            
for result in results:
    record = list()
    record.append(result[0])
    record.append(result[1])
    record.append(results[result])
    resultList.append(record)

resultsDf = pd.DataFrame(resultList)
resultsDf.columns = ['descrip1',  'descrip2', 'similarity']
Utilities.createTable(userfolder, resultsDf, host, database, username, password,  schema, "sim_scores", primaryKeyColumns=['descrip1', 'descrip2'])
    

