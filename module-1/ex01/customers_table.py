import psycopg2


def customers_table(cur):
    """
    Joins all the customers table into one
    """

    # Query the db for table names matching pattern "data_202*_***"
    cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'data_202%_%'")
    table_names = [row[0] for row in cur.fetchall()]

    # Start SQL command for union of all matching tables
    sql_command = "CREATE TABLE customers AS "

    # Add each table to SQL command
    for i, table_name in enumerate(table_names):
        if i > 0:
            sql_command += " UNION ALL "
        sql_command += f"SELECT * FROM {table_name}"

    # Execute the SQL command
    cur.execute(sql_command)
    print("All 'data_202*_***' tables successfully joined into 'customers' table.")


def main():
    """
    Sets db connection
    Calls function to join all customers table into one
    Commits changes
    Closes connection
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

    # join all customers tables
    customers_table(cur)

    # Commit changes and close connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
