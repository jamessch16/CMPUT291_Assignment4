# Writien by: Adit Rada
# Date: 23-Mar-21

import sqlite3
import random
import time

def main():
    # Run required tests before the index is created
    print("Running tests on query 1:\n")
    doTestsOnQuery(runTestOnQuery1)
    print("\n", "-"*80, "\n", "Running tests on query 2:\n")
    doTestsOnQuery(runTestOnQuery2)

    # Create Index on needsPart
    dropIndex()
    print("\n", "\n", "*"*80, "\n", "Creating index on needsPart...")
    createIndex()

    # Run required tests after the index is created
    print("Running tests after index has been created")
    print("Running tests on query 1:\n")
    doTestsOnQuery(runTestOnQuery1)
    print("\n", "-"*80, "\n", "Running tests on query 2:\n")
    doTestsOnQuery(runTestOnQuery2)   

    # Drop Index on needsPart
    dropIndex()



def getPartNumber(cur):
    # Effect: This function return a random partNumber form the database
    # Parameters: cur - cursor to the database
    # Return: partNumber:Integer
    done = False
    while (not done):
        cur.execute("""SELECT partNumber
                    FROM Parts
                    ORDER BY random()  
                    LIMIT 1;""")
        partNumbers = cur.fetchall()[0][0]
        if (isinstance(partNumbers, int)):
            done = True
    return partNumbers

def getNeedsPart(cur):
    # This function return a random needsPart number form the database
    # Parameters: cur - cursor to the database
    # Return: needsPart:Integer
    done = False
    while (not done):
        cur.execute("""SELECT needsPart
                    FROM Parts
                    ORDER BY random()  
                    LIMIT 1;""")
        needsPart = cur.fetchall()[0][0]
        if (isinstance(needsPart, int)):
            done = True
    return needsPart

def query1(cur, U):
    # Effect: Given a randomly selected UPC code U from the UPC database that exist in Parts 
    #         find the price of part in Parts that has partNumber = U
    # Parameters: cur - cursor to the database
    #             U - partNumber:Integer
    cur.execute("""select partPrice
                   from Parts
                   where partNumber = :U;""", {'U': int(U)})
    price = cur.fetchall()
    return price

def query2(cur, U):
    # Efffect: Given a randomly selected UPC code U from the UPC database that exist in Parts 
    #          find the price of part in Parts that has needsPart = U
    # Parameters: cur - cursor to the database
    #             U - needsPart:Integer
    cur.execute("""select partPrice
                   from Parts
                   where needsPart = :U;""", {'U': int(U)})
    prices = cur.fetchall()
    return prices

def runTestOnQuery1(cur):
    # Effect: This function runs query 1 100 times on the given database
    # Retrun: Average time for the 100 queries on that database
    total_time = 0
    for i in range(100):
        U = getPartNumber(cur)
        start_time = time.time()
        query1(cur, U)
        total_time += (time.time() - start_time)
    return total_time / 100

def runTestOnQuery2(cur):
    # Effect: This function runs query 2 100 times on the given database
    # Retrun: Average time for the 100 queries on that database
    total_time = 0
    for i in range(100):
        U = getNeedsPart(cur)
        start_time = time.time()
        query2(cur, U)
        total_time += (time.time() - start_time)
    return total_time / 100

def doTestsOnQuery(runTestOnQuery):
    # Effect: Runs test against each of the databases and prints out the results
    # Paramemter: runTestOnQuery function, which is a function pointer to runTestOnQuery1 or runTestOnQuery2
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            print("Opening database: ", db)
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            avg_time = runTestOnQuery(cur)
            print("The average time after running query  100 times on " + db + " is: ", avg_time)
        finally:
            print("Closing database: ", db, "\n")
            conn.close()

def createIndex():
    # Effect: Creates index on needsPart on each of the databases
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            cur.execute("""CREATE INDEX idxNeedsPart ON Parts ( needsPart )""")
        finally:
            conn.close()
    
def dropIndex():
    # Effect: Drops index on needsPart on each of the databases
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            cur.execute("""DROP INDEX IF EXISTS idxNeedsPart;""")
        finally:
            conn.close()

main()