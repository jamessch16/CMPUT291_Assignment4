# Writien by: Adit Rada
# Date: 24-Mar-21

import sqlite3
import random
import time

def main():
    # # Run required tests before the index is created
    # print("Running tests on query 3:\n")
    # doTestsOnQuery()

    # Create Index on needsPart
    dropIndex()
    print("\n", "\n", "*"*80, "\n", "Creating index on madeIn...")
    createIndex()

    # Run required tests after the index is created
    print("Running tests after index has been created")
    print("Running tests on query 1:\n")
    doTestsOnQuery()

    # Drop Index on needsPart
    dropIndex()

    # try:
    #     conn = sqlite3.connect("A4v100.db")
    #     cur = conn.cursor()
    #     print(query3(cur))
    # finally:
    #     conn.close()

def getCountry(cur):
    # Effect: This function return a random country code from the database
    # Parameters: cur - cursor to the database
    # Return: madeIn:Text
    cur.execute("""SELECT madeIn
                   FROM Parts
                   ORDER BY random()  
                   LIMIT 1;""")
    country = cur.fetchall()
    return country[0][0]

def query3(cur):
    # Effect: find the average price of the parts made in each country 
    # Parameters: cur - cursor to the database
    # Return: list of each country and average part price for parts made in the country
    cur.execute("""select madeIn, avg(partPrice)
                   from Parts
                   group by madeIn;""")
    avg_price = cur.fetchall()
    return avg_price

def runTestOnQuery3(cur):
    # Effect: This function runs query 3 100 times on the given database
    # Retrun: Average time for the 100 queries on that database
    total_time = 0
    for i in range(100):
        start_time = time.time()
        query3(cur)
        total_time += (time.time() - start_time)
    return total_time / 100

def doTestsOnQuery():
    # Effect: Runs test against each of the databases and prints out the results
    # Paramemter: runTestOnQuery function, which is a function pointer to runTestOnQuery1 or runTestOnQuery2
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            print("Opening database: ", db)
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            avg_time = runTestOnQuery3(cur)
            print("The average time after running query  100 times on " + db + " is: ", avg_time)
        finally:
            print("Closing database: ", db, "\n")
            conn.close()

        
def createIndex():
    # Effect: Creates index on madeIn on each of the databases
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            cur.execute("""CREATE INDEX idxMadeIn ON Parts ( MadeIn );""")
        finally:
            conn.close()
    
def dropIndex():
    # Effect: Drops index on madeIn on each of the databases
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            cur.execute("""DROP INDEX IF EXISTS idxMadeIn;""")
        finally:
            conn.close()
main()