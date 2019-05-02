import HashUtility
import Utilities
database= "postgres"
username = "postgres"
password= "docker"
schema = "public"
host = "localhost"
userfolder = ""


tran_descriptions = list(Utilities.runPgQuery(host, database, username, password, "select distinct description from public.merged_trans")['description'])


print("calculating idfs")
idfDict = HashUtility.computeAllIdf(tran_descriptions)
print("calculating tf idfs")
tfIdfDict = HashUtility.computeAllTfIdf(tran_descriptions, idfDict)

s1= "INSTACART"
s2= "INSTACART SUBSCRIPTION"

print(str(HashUtility.softTfIdf(s1, s2, idfDict, tfIdfDict)))
