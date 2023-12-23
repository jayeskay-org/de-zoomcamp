from time import time
from sqlalchemy.engine import URL, create_engine
from sqlalchemy import text
from prefect import flow, task
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


@task(log_prints=True, retries=3)
def download_data(url) -> pd.DataFrame:
    # Download parquet file
    os.system(f"pwd")
    os.system(f"wget {url} -O source.parquet")

    pf = pq.ParquetFile('source.parquet')

    return pf


@task(log_prints=True, retries=3)
def create_table(engine, pf, table) -> None:
    # Pull in subset of source data
    df = pf.read().to_pandas()[:100]

    # Clean up columns, cast date/time strings
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Create connection object to create schema, drop table, recreate using defined schema
    with engine.connect() as conn:
        # Create schema string
        s = pd.io.sql.get_schema(df, name=table, con=conn)

        # Drop table (if exists); create using schema string (as SQLAlchemy text)
        conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
        conn.execute(text(s))


@task(log_prints=True, retries=3)
def ingest_data(parquet_file, batch_size, table, engine):
    counter = 0

    t_start = time()

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        t_i_start = time()

        try:
            df_i = batch.to_pandas()
        except Exception as err:
            print(f"Unexpected {err}, {type(err)=}")
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


@flow(name='Flow: Ingest')
def main_flow():
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5431'
    database = 'ny_taxi'
    table = 'yellow_taxi_data'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

    parquet_file = download_data(url)
    engine = PostgreSQL(user, password, host, port, database).get_engine()
    create_table(engine, parquet_file, table)
    ingest_data(parquet_file=parquet_file, batch_size=100000, table=table, engine=engine)


if __name__ == '__main__':
    main_flow()
