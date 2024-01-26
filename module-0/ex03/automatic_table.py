import os
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
            event_time TIMESTAMP WITHOUT TIME ZONE,
            event_type VARCHAR(255) NOT NULL,
            product_id INTEGER,
            price NUMERIC,
            user_id BIGINT,
            user_session TEXT
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


def main():
    """
    Sets db connection
    Run the function import_data for every csv file on the customer dir
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

    # Get all files on the customer dir
    directory_path = "../../subject/customer/"
    file_list = os.listdir(directory_path)

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    for file_name in file_list:
        file_path = os.path.join(directory_path, file_name)
        print("----------------------------------------")
        print(file_path)
        table_name = file_name.split('.')[0]
        import_data(cur, table_name, file_path)
        print('\n')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
