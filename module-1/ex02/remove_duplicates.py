import psycopg2


def remove_duplicates(cur):
    """
    Removes duplicates from 'customers' table
    """

    # Add a new column with the event_time rounded to the nearest minute
    cur.execute("""
        ALTER TABLE customers
        ADD COLUMN event_time_minute TIMESTAMP WITHOUT TIME ZONE
        GENERATED ALWAYS AS (date_trunc('minute', event_time)) STORED
    """)

    # Create a new table without duplicates
    cur.execute("""
        CREATE TABLE customers_no_duplicates AS
        SELECT DISTINCT ON (event_time_minute, event_type, product_id, price, user_id, user_session)
        *
        FROM customers
        ORDER BY event_time_minute, event_type, product_id, price, user_id, user_session, event_time DESC
    """)

    # replace the original table for the new one
    cur.execute("DROP TABLE customers")
    cur.execute("ALTER TABLE customers_no_duplicates RENAME TO customers")
    print("Removed duplicates from 'customers' table.")


def main():
    """
    Sets db connection
    Remove duplicates
    Commit changes
    Close connection
    """

    # Connection parameters
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'database': 'piscineds',
        'user': 'rdas-nev',
        'password': 'mysecretpassword'
    }

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Remove duplicates
    remove_duplicates(cur)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
