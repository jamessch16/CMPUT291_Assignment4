import sqlite3
import time

def main():

    # print("Running tests on query 5 and 6:\n")
    # doTestsOnQuery()
    doTestsOnQuery()

    # Create Index on needsPart
    print("\n", "\n", "*"*80, "\n", "Creating index on needsPart...")
    createIndex()

    # Run required tests after the index is created
    print("Running tests after index has been created")
    print("Running tests on query 6\n")
    doTestsafterIndex()

    # Drop Index on needsPart
    dropIndex()

def query5(cur):
    # Effect: Considering the parts that that exist in Parts, find the quantity of parts that are not used in any other
    # part, your query must use NOT EXISTS
    # Parameters: cur - cursor to the database
    # Return: Number of parts that are not used in any other parts
    cur.execute("""select count(1)
                   from Parts P
                   where not exists(select * from Parts Q where P.partNumber = Q.needsPart);""")
    count_parts = cur.fetchall()
    return count_parts

def query6(cur):

    # Effect: Considering the parts that that exist in Parts, find the quantity of parts that are not used in any other
    # part, your query must use NOT IN
    # Parameters: cur - cursor to the database
    # Return: Number of parts that are not used in any other parts
    cur.execute("""select count(1)
                   from Parts P 
                   where P.partNumber not in (select Q.needsPart from Parts Q);""")
    count_parts = cur.fetchall()
    return count_parts

def runTestOnQuery5(cur, number):

    # Effect: This function runs query 5 "number" times on the given database
    # Return: Average time for the "number" queries on that database
    total_time = 0
    for i in range(number):
        start_time = time.time()
        query5(cur)
        total_time += (time.time() - start_time)
    return total_time / number

def runTestOnQuery6(cur, number):
    # Effect: This function runs query 6 "number" times on the given database
    # Retrun: Average time for the "number" queries on that database
    total_time = 0
    for i in range(number):
        start_time = time.time()
        query6(cur)
        total_time += (time.time() - start_time)
    return total_time / number

def doTestsOnQuery():

    # Effect: Runs test against each of the databases and prints out the results
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db"]

    # Running 50 times and calculating average time for the first 3 databases
    for db in databases:
        try:
            print("Opening database: ", db)
            conn = sqlite3.connect(db)
            cur = conn.cursor()

            # Printing average times for each query for the first 3 databases
            avg_time = runTestOnQuery5(cur, 50)
            print("The average time after running query 5 50 times on " + db + " is: ", avg_time)

            avg_time = runTestOnQuery6(cur,50)
            print("The average time after running query 6 50 times on " + db + " is: ", avg_time)

        finally:
            print("Closing database: ", db, "\n")
            conn.close()

    # Running 10 times and calculating average time for 100k database only for Query 6
    try:
        print("Opening database: ", "A4v100k.db")
        conn = sqlite3.connect("A4v100k.db")
        cur = conn.cursor()
        avg_time = runTestOnQuery6(cur, 10)
        print("The average time after running query 6 10 times on " + "A4v100k.db" + " is: ", avg_time)
    finally:
        print("Closing database: ", "A4v100k.db", "\n")
        conn.close()

    # Running 5 times and calculating average time for 1M database only for Query 6
    try:
        print("Opening database: ", "A4v1M.db")
        conn = sqlite3.connect("A4v1M.db")
        cur = conn.cursor()
        avg_time = runTestOnQuery6(cur, 5)
        print("The average time after running query 6 5 times on " + "A4v1M.db" + " is: ", avg_time)
    finally:
        print("Closing database: ", "A4v1M", "\n")
        conn.close()

def doTestsafterIndex():
    # Effect: Runs test against each of the databases after indexing and prints out the results
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db"]

    # Running 50 times and calculating average time for the first 3 databases only for Query 6
    for db in databases:
        try:
            print("Opening database: ", db)
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            avg_time = runTestOnQuery6(cur, 50)
            print("The average time after running query 5 50 times on " + db + " is: ", avg_time)
        finally:
            print("Closing database: ", db, "\n")
            conn.close()

    # Running 10 times and calculating average time for 100k database only for Query 6
    try:
        print("Opening database: ", "A4v100k.db")
        conn = sqlite3.connect("A4v100k.db")
        cur = conn.cursor()
        avg_time = runTestOnQuery6(cur, 10)
        print("The average time after running query 6 10 times on " + "A4v100k.db" + " is: ", avg_time)
    finally:
        print("Closing database: ", "A4v100k.db", "\n")
        conn.close()

    # Running 5 times and calculating average time for 1M database only for Query 6
    try:
        print("Opening database: ", "A4v1M.db")
        conn = sqlite3.connect("A4v1M.db")
        cur = conn.cursor()
        avg_time = runTestOnQuery6(cur, 5)
        print("The average time after running query 6 5 times on " + "A4v1M.db" + " is: ", avg_time)
    finally:
        print("Closing database: ", "A4v1M", "\n")
        conn.close()

def createIndex():
    # Effect: Creates index on madeIn on each of the databases
    databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
    for db in databases:
        try:
            conn = sqlite3.connect(db)
            cur = conn.cursor()
            cur.execute("""CREATE INDEX idxneedsPart ON Parts ( needsPart );""")
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