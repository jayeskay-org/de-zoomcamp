from time import time
from sqlalchemy.engine import URL, create_engine
import pandas as pd
import pyarrow.parquet as pq


# Create PostgreSQL class with connection credentials
class PostgreSQL:
    def __init__(self, database):
        self.drivername = 'postgresql+psycopg2'
        self.user = 'root'
        self.password = 'root'
        self.host = 'localhost'
        self.port = 5431
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
def get_engine(database):
    credentials = PostgreSQL(database).get_credentials()
    url = URL.create(**credentials)
    engine = create_engine(url)

    return engine


if __name__ == '__main__':
    # Construct sqlalchemy engine object
    engine = get_engine('ny_taxi')

    # Pull in parquet file -> pd.DataFrame(); reduce to first 100 rows
    tb = pq.read_table('/Users/jonathon/GitHub/de-zoomcamp/week_1_basics_n_setup/2_docker_sql/yellow_tripdata_2021-01.parquet').to_pandas()[:100]

    # Clean up columns, cast date/time strings
    tb.columns = tb.columns.str.lower().str.replace(' ', '_')
    tb['tpep_pickup_datetime'] = pd.to_datetime(tb['tpep_pickup_datetime'])
    tb['tpep_dropoff_datetime'] = pd.to_datetime(tb['tpep_dropoff_datetime'])

    # Get schema based upon 
    s = pd.io.sql.get_schema(tb, name='yellow_taxi_data', con=engine.connect())
    print(s)

    # Create connection object to drop table, recreate using defined schema
    with engine.connect() as conn:
        conn.execute('DROP TABLE IF EXISTS yellow_taxi_data;')
        conn.execute(s)

    # Batch load parquet file to database
    parquet_file = pq.ParquetFile('/Users/jonathon/GitHub/de-zoomcamp/week_1_basics_n_setup/2_docker_sql/yellow_tripdata_2021-01.parquet')
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
            df_i.to_sql(name='yellow_taxi_data', con=engine.connect(), if_exists='append', index=False)

        t_i_end = time()

        print(f"{batch_size} rows inserted in {round(t_i_end - t_i_start, 4)} seconds")

    t_end = time()

    print(f"{counter} rows inserted in {round(t_end - t_start, 4)} seconds")
