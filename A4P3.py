import sqlite3
import time
import random

databases = ["A4v100.db", "A4v1k.db", "A4v10k.db", "A4v100k.db", "A4v1M.db"]
test_size = [50, 50, 50, 10, 5]

def main():

    # Task 9
    print("Executing Task 9")
    for i in range(5):

        print("Opening", databases[i])

        db = sqlite3.connect(databases[i])
        cur = db.cursor()
        dropIndex(cur)

        print("Average query time for Query Q4:", query4TimeTest(cur, test_size[i]))

        print("Closing", databases[i])
        cur.close()
        db.close()

    # Task 10
    print("\nCreating Index\n")
    for i in range(5):

        db = sqlite3.connect(databases[i])
        cur = db.cursor()

        createIndex(cur)

        cur.close()
        db.close()

    # Task 11
    print("Executing Task 11")
    for i in range(5):

        print("Opening", databases[i])

        db = sqlite3.connect(databases[i])
        cur = db.cursor()

        print("Average query time for Query Q4:", query4TimeTest(cur, test_size[i]))

        print("Closing", databases[i])
        cur.close()
        db.close()


def query4TimeTest(cur, sample_size):
    # Effect: This function runs query 4 [sample_size] times on the given database
    # Return: Average time for queries on that database
    codes = getMadeInArray(cur, sample_size)
    start_time = 0
    end_time = 0
    total_time = 0

    for i in range(sample_size):
        C = codes[i]
        start_time = time.time()
        query4(cur, C)
        end_time = time.time()
        total_time += end_time - start_time
    
    return total_time/sample_size
    

def query4(cur, C):
    # Effect: Find the most expensive part made in a randomly selected country (code) that exist in Parts. 
    # Parameters: cur - cursor to the database
    #             U - partNumber:Integer
    # Return: The id of the most expensive part
    cur.execute("SELECT p.partNumber \
                 FROM Parts p \
                 WHERE p.madeIn = :C \
                 AND p.partPrice = ( \
                    SELECT MAX(q.partPrice) \
                    FROM Parts q \
                    WHERE q.madeIn = :C) \
                 LIMIT 1;",
                {"C":C})
    
    return cur.fetchall()
    

def getMadeInArray(cur, size):
    # Effect: This function returns an array of random madeIn from the database
    # Parameters: cur - cursor to the database
    #             size - size of the array
    # Return: partNumber:Integer

    cur.execute("SELECT madeIn FROM Parts;")
    query = cur.fetchall()

    array = []
    for i in range(size):
        rand = random.randint(0, len(query) - 1)
        array.append(query[rand][0])

    return array


def createIndex(cur):
    # Effect: Creates index on needsPart on each of the databases
    cur.execute("CREATE INDEX MaxCost ON Parts (madeIn, partPrice);")

    
def dropIndex(cur):
    # Effect: Drops index on needsPart on each of the databases
    cur.execute("DROP INDEX IF EXISTS MaxCost;")


if __name__ == "__main__":
    main()