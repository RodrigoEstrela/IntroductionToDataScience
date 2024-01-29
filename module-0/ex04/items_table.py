import psycopg2


def import_data(cur, table_name, csv_file_path):
    """
    Imports the data from a csv file to a postgresql table
    """

    # Check if the table already exists
    cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s)", (table_name,))
    if cur.fetchone()[0]:
        print(f"Table '{table_name}' already exists. Skipping import.")
        return

    # Create the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {} (
            product_id INTEGER,
            category_id BIGINT,
            category_code TEXT,
            brand TEXT
            )
    """.format(table_name))

    # Open the csv file and import the data into the table
    with open(csv_file_path, 'r') as f:
        cur.copy_expert(
            "COPY {} FROM STDIN WITH CSV HEADER".format(table_name),
            f
        )
    print(f"Data from {csv_file_path} successfully imported into {table_name} table.")

    # Print the number of rows in the table
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cur.fetchone()[0]
    print(f"Imported {row_count} rows into {table_name} table.")


def clean_up_data(cur, table_name):
    """
    Cleans up the data in a postgresql table
    """

    # SQL query to delete rows where product_id, category_id, or brand is NULL
    sql_query = """
        DELETE FROM {}
        WHERE product_id IS NULL OR category_id IS NULL OR brand IS NULL
    """.format(table_name)

    # Execute the SQL query
    cur.execute(sql_query)
    print(f"Cleaned up data in {table_name} table.")

    # Print the number of rows in the table
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cur.fetchone()[0]
    print(f"{row_count} rows now on {table_name} table.")


def main():
    """
    Sets db connection
    Calls import data function
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

    # Csv file path
    csv_file_path = '../../subject/item/item.csv'

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Import data
    import_data(cur, "items", csv_file_path)

    # Clean up data
    clean_up_data(cur, "items")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
