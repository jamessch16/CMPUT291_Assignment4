import sqlite3

db_100 = "./A4v100.db/"
db_1k = "./A4v1k.db/"
db_10k = "./A4v10k.db/"
db_100k = "./A4v100k.db/"
db_1m = "./A4v1M.db/"

def main():
    pass


def generate_db(db_cursor, num_samples):
    pass


def create_parts_table(db_cursor):
    db_cursor.execute(  "CREATE TABLE Parts (                               \
                            partNumberINTEGER, -- a UPC code                \
                            partPriceINTEGER, -- in the [1, 100] range      \
                            needsPartINTEGER, -- a UPC code                 \
                            madeInTEXT, -- a country (2 letters) code       \
                            PRIMARY KEY(partNumber)                         \
                        );")

if __name__ == "__main__":
    main()