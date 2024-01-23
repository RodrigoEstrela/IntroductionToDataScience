import os
import pandas as pd
from sqlalchemy import create_engine, types


def create_table(path: str):
    """
    Creates a sql table based on the csv file provided by path
    """
    db_connection = "postgresql://rdas-nev:mysecretpassword@localhost/piscineds"
    engine = create_engine(db_connection)

    df = pd.read_csv(path)

    table_name = "items"

    column_types = {
        'product_id': types.INTEGER,
        'category_id': types.BIGINT,
        'category_code': types.String,
        'brand': types.String
    }

    df.to_sql(table_name, engine, index=False, if_exists='replace', dtype=column_types)

    print(f"Table '{table_name}' created successfully.")


def main():
    """
    Gets the csv path
    Runs the create_table function
    """
    directory_path = "item/"
    file_path = os.path.join(directory_path, "item.csv")
    create_table(file_path)


if __name__ == '__main__':
    main()
