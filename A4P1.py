import sqlite3

db_100 = "./A4v100.db/"
db_1k = "./A4v1k.db/"
db_10k = "./A4v10k.db/"
db_100k = "./A4v100k.db/"
db_1m = "./A4v1M.db/"

def main():
    pass


def find_price_part(db_cursor, upc_code):
    db_cursor.execute( "SELECT partPrice  \
                        FROM Parts  \
                        WHERE partNumber = :upc_code",
                        {"upc_code":upc_code})


def find_price_needs(db_cursor, upc_code):
    db_cursor.execute( "SELECT partPrice  \
                        FROM Parts  \
                        WHERE needsPart = :upc_code",
                        {"upc_code":upc_code})


def create_index(db_cursor):
    db_cursor.execute( "CREATE INDEX idxNeedsPart ON Parts ( needsPart );")


if __name__ == "__main__":
    main()