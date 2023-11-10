from time import time
from sqlalchemy.engine import URL, create_engine
from argparse import ArgumentParser
import os
import pandas as pd
import pyarrow.parquet as pq


# Create PostgreSQL class with connection credentials
class PostgreSQL:
    def __init__(self, user, password, host, port, database):
        self.drivername = 'postgresql+psycopg2'
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database


    def get_credentials(self):
        return \
        (
            {
                'drivername': self.drivername,
                'username': self.user,
                'password': self.password,
                'host': self.host,
                'port': self.port,
                'database': self.database
            }
        )


    # Define sqlalchemy engine object
    def get_engine(self):
        credentials = self.get_credentials()
        url = URL.create(**credentials)
        engine = create_engine(url)

        return engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url

    # Call PostgreSQL class object
    postgres = PostgreSQL(user, password, host, port, database)

    # Construct sqlalchemy engine object
    engine = postgres.get_engine()

    # Download parquet file
    os.system(f"pwd")
    os.system(f"wget {url} -O source.parquet")

    # Pull in parquet file -> pd.DataFrame(); reduce to first 100 rows
    tb = pq.read_table('source.parquet').to_pandas()[:100]

    # Clean up columns, cast date/time strings
    tb.columns = tb.columns.str.lower().str.replace(' ', '_')
    tb['tpep_pickup_datetime'] = pd.to_datetime(tb['tpep_pickup_datetime'])
    tb['tpep_dropoff_datetime'] = pd.to_datetime(tb['tpep_dropoff_datetime'])

    # Get schema based upon 
    s = pd.io.sql.get_schema(tb, name=table, con=engine.connect())
    print(s)

    # Create connection object to drop table, recreate using defined schema
    with engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table};")
        conn.execute(s)

    # Batch load parquet file to database
    parquet_file = pq.ParquetFile('source.parquet')
    batch_size = 100000
    counter = 0

    t_start = time()

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        t_i_start = time()

        try:
            df_i = batch.to_pandas()
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
        else:
            df_i.columns = df_i.columns.str.replace(' ', '_').str.lower()
            df_i['tpep_pickup_datetime'] = pd.to_datetime(df_i['tpep_pickup_datetime'])
            df_i['tpep_dropoff_datetime'] = pd.to_datetime(df_i['tpep_dropoff_datetime'])
        finally:
            counter += len(df_i)
            df_i.to_sql(name=table, con=engine.connect(), if_exists='append', index=False)

        t_i_end = time()

        print(f"{batch_size} rows inserted in {round(t_i_end - t_i_start, 4)} seconds")

    t_end = time()

    print(f"{counter} rows inserted in {round(t_end - t_start, 4)} seconds")


if __name__ == '__main__':
    parser = ArgumentParser(
        prog='INGEST_DATA',
        description='Ingest .parquet file to PostgreSQL'
    )

    parser.add_argument('--user')       # root
    parser.add_argument('--password')   # root
    parser.add_argument('--host')       # localhost
    parser.add_argument('--port')       # 5431
    parser.add_argument('--database')   # ny_taxi
    parser.add_argument('--table')      # yellow_taxi_data
    parser.add_argument('--url')        # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

    args = parser.parse_args()

    main(params=args)
