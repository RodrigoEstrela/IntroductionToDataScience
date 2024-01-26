import psycopg2


def import_data(cur, table_name, csv_file_path):
    """
    Imports the data from a csv file to a postgresql table
    """

    # Create the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {} (
            event_time TIMESTAMP WITHOUT TIME ZONE,
            event_type VARCHAR(255) NOT NULL,
            product_id INTEGER,
            price NUMERIC,
            user_id BIGINT,
            user_session TEXT
            )
    """.format(table_name))

    # Open the csv file
    with open(csv_file_path, 'r') as f:
        cur.copy_expert(
            "COPY {} FROM STDIN WITH CSV HEADER".format(table_name),
            f
        )
    print(f"Data from {csv_file_path} successfully imported into {table_name} table.")


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

    # Csv file path and table name
    csv_file_path = '../../subject/customer/data_2022_oct.csv'
    table_name = csv_file_path.split('/')[-1].split('.')[0]

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Import data
    import_data(cur, table_name, csv_file_path)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
