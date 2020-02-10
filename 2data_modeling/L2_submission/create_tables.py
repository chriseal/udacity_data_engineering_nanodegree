import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ Establish a connection to the server and instantiate an empty database. 
        
    Returns:
        cur: The postgres database cursor object
        conn: The postgres database connection object
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ Drops all tables by running all SQL commands in drop_table_queries global variable.

    Args:
        cur: The postgres database cursor object
        conn: The postgres database connection object
    """
    print("drop_tables")
    for i, query in enumerate(drop_table_queries):
        print("\t{}".format(i))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Instantiates all tables by running all create table commands in create_table_queries global variable.

    Args:
        cur: The postgres database cursor object
        conn: The postgres database connection object
    """
    
    print("create_tables")
    for i, query in enumerate(create_table_queries):
        print("\t{}".format(i))
        cur.execute(query)
        conn.commit()


def main():
    """ Drops all relevant existing tables and instantiates new ones. """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()