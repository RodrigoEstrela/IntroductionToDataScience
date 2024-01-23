import pandas as pd
from sqlalchemy import create_engine, types

db_connection = "postgresql://rdas-nev:mysecretpassword@localhost/piscineds"
engine = create_engine(db_connection)

csv_file_path = "data_2022_oct.csv"

df = pd.read_csv(csv_file_path)

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
