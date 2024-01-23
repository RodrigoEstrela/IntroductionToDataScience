import os

import pandas as pd
from sqlalchemy import create_engine, types


def create_table(path: str):
    """
    Creates a sql table based on the csv file provided by path
    """
    db_connection = "postgresql://rdas-nev:mysecretpassword@localhost/piscineds"
    engine = create_engine(db_connection)

    csv_file_path = path
    df = pd.read_csv(path)

    table_name = csv_file_path.split("/")[-1].split(".")[0]
    df['event_time'] = pd.to_datetime(df['event_time'])

    column_types = {
        'event_time': types.TIMESTAMP,
        'event_type': types.VARCHAR,
        'product_id': types.INTEGER,
        'price': types.NUMERIC,
        'user_id': types.BIGINT,
        'user_session': types.UUID
    }

    df.to_sql(table_name, engine, index=False, if_exists='replace', dtype=column_types)

    print(f"Table '{table_name}' created successfully.")


def main():
    """
    Run the function create_table for every csv file on the customer dir
    """
    directory_path = "customer/"
    file_list = os.listdir(directory_path)

    for file_name in file_list:
        file_path = os.path.join(directory_path, file_name)
        print(file_path)
        create_table(file_path)


if __name__ == '__main__':
    main()
