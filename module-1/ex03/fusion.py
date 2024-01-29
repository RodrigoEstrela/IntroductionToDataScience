import psycopg2


def main():
    """
    Sets db connection
    Combine customers and items tables
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

    # SQL query to create a new table that includes the joined data
    sql_query = """
        CREATE TABLE customers_new AS
        SELECT c.*, i.category_id, i.category_code, i.brand
        FROM customers c
        LEFT JOIN items i ON c.product_id = i.product_id;
    """
    cur.execute(sql_query)
    print("customers table and items table joined in customers table.")

    # SQL query to drop the old customers table
    sql_query = "DROP TABLE customers;"
    cur.execute(sql_query)

    # SQL query to rename the new table to customers
    sql_query = "ALTER TABLE customers_new RENAME TO customers;"
    cur.execute(sql_query)

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()