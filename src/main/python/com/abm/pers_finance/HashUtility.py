import math
import random

class HashFunc:
    
    hashFunct = list()
    
    def __init__(self, bins, p):
        self.b = bins
        self.p = p       
        if p<bins:
            print("Invalid hash params")

    def hash(self, token, i):
        while len(self.hashFunct)<i:
            self.hashFunct.append(list())
        j=1
        x=0
        for c in token:
            val = ord(c)
            if len(self.hashFunct[i-1])<j:
                self.hashFunct[i-1].append(random.randint(self.p+1, self.p*2-1))
            x=x+self.hashFunct[i-1][j-1]*val
            j=j+1
        x=(x%self.p)%self.b
        return x

    def __str__(self):
        i=1
        out=""
        for funct in self.hashFunct:
            out = out+"function: " + str(i) + "\n"
            j=1
            for a in funct:                   
                out= out+"coefficient " + str(j) + ": " + str(a) + "\n"
                j=j+1
            i=i+1
        return out
                
                
def tf(token, string):
    if string is None:
        return 0
    return string.count(token)


def computeAllIdf(stringList):
    IdfDict = dict()
    for string in stringList:
        if string is not None:
            for token in string.split():
                if token not in IdfDict.keys():
                    IdfDict[token] = 1
                else:
                    IdfDict[token] = IdfDict[token] + 1
    return IdfDict


def idf(token, stringList):
    runSum = 0
    for string in stringList:
        runSum = runSum + tf(token, string)

    return len(names) / runSum


def tf_idf(token, string, idfDict):
    return math.log(tf(token, string) + 1) * math.log(idfDict[token])


def computeAllTfIdf(stringList, IdfDict):
    addressDict = dict()
    for string in stringList:
        if string is not None:
            for token in string.split():
                if string not in addressDict:
                    addressDict[string] = dict()
                    addressDict[string][token] = tf_idf(token, string, IdfDict)
                else:
                    if token not in addressDict[string].keys():
                        addressDict[string][token] = tf_idf(token, string, IdfDict)

    return addressDict


def tfIdfNorm(string, tfIdfDict):
    runSum = 0
    for token in string.split():
        runSum = runSum + math.pow(tfIdfDict[string][token], 2)

    return math.sqrt(runSum)


def cosineSimilarity(string1, string2, tfIdfDict):
    if string1 is None or string2 is None:
        return list()
    tokens1 = string1.split()
    tokens2 = string2.split()
    runSum = 0

    for token in tokens1:
        if token in tokens2:
            runSum = runSum + math.pow(tfIdfDict[token], 2)

    return (runSum / (tfIdfNorm(string1, tfIdfDict) *
                      tfIdfNorm(string2, tfIdfDict)))


def iterative_levenshtein(s, t):
    """
        iterative_levenshtein(s, t) -> ldist
        ldist is the Levenshtein distance between the strings
        s and t.
        For all i and j, dist[i,j] will contain the Levenshtein
        distance between the first i characters of s and the
        first j characters of t
    """
    rows = len(s) + 1
    cols = len(t) + 1
    dist = [[0 for x in range(cols)] for x in range(rows)]
    # source prefixes can be transformed into empty strings
    # by deletions:
    for i in range(1, rows):
        dist[i][0] = i
    # target prefixes can be created from an empty source string
    # by inserting the characters
    for i in range(1, cols):
        dist[0][i] = i

    for col in range(1, cols):
        for row in range(1, rows):
            if s[row - 1] == t[col - 1]:
                cost = 0
            else:
                cost = 1
            dist[row][col] = min(dist[row - 1][col] + 1,  # deletion
                                 dist[row][col - 1] + 1,  # insertion
                                 dist[row - 1][col - 1] + cost)  # substitution

    return dist[row][col]


def longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in range(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return len(s1[x_longest - longest: x_longest])


class StringCompare():
    def __init__(self, s1, s2, sim):
        self.s1 = s1
        self.s2 = s2
        self.sim = sim

    def __str__(self):
        return (self.s1 + ", " + self.s2 + ", " + str(self.sim))


def closeTokens(s1, s2):
    if (s1 is None) or (s2 is None):
        return list()
    tokens1 = s1.split()
    tokens2 = s2.split()
    s1SimDict = dict()
    for token1 in tokens1:
        maxSim = 0
        for token2 in tokens2:
            theta = min(len(token1), len(token2)) * .5
            longestSubstring = longest_common_substring(token1, token2)
           
            if (longestSubstring > theta):
                s1SimDict[token1] = StringCompare(token1, token2, longestSubstring)
                
    return s1SimDict.values()


def softTfIdf(s1, s2, idfDict, tfIdfDict):
    t1_close = closeTokens(s1, s2)
    runSum = 0
    for token in t1_close:
        norm1 = tfIdfNorm(s1, tfIdfDict)
        norm2 = tfIdfNorm(s2, tfIdfDict)
        #print(str(norm1))
        #print(str(norm2))
        if norm1>0 and norm2>0:
            #print("s1: " + token.s1)
            #print("s2: " + token.s2)
            #print("sim: " + str(token.sim))
            database= "postgres"
            #print("tf idf s2: " + str(tfIdfDict[s2][token.s2]))
            #print(tfIdfDict[s2][token.s2])
            runSum = (runSum + tfIdfDict[s1][token.s1] / norm1 *
                  tfIdfDict[s2][token.s2] / norm2 * token.sim)
    return runSum

