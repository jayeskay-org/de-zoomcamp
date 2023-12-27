from time import time
from sqlalchemy import text
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_aws import AwsCredentials, S3Bucket
import os
import pandas as pd
import pyarrow.parquet as pq
import pandas_redshift as pr


# Create AWSLoader class with connection credentials for Redshift
class AWSLoader:
    def __init__(self, connector_block: SqlAlchemyConnector, credentials_block: AwsCredentials):
        sqlalchemy_params = [attr[1] for attr in connector_block.connection_info]

        self.driver = sqlalchemy_params[0]
        self.database = sqlalchemy_params[1]
        self.user = sqlalchemy_params[2]
        self.password = sqlalchemy_params[3]
        self.host = sqlalchemy_params[4]
        self.port = sqlalchemy_params[5]

        aws_params = [credentials_block.aws_access_key_id, credentials_block.aws_secret_access_key]

        self.aws_access_key_id = aws_params[0]
        self.aws_secret_access_key = aws_params[1]


    def get_sqlalchemy_credentials(self):
        return \
        (
            {
                'dbname': self.database,
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'password': self.password.get_secret_value(),
            }
        )


    def get_aws_credentials(self):
        return \
        (
            {
                'aws_access_key_id': self.aws_access_key_id,
                'aws_secret_access_key': self.aws_secret_access_key.get_secret_value()
            }
        )


    # Define SQLAlchemy engine object
    def connect_to_redshift(self) -> None:
        sqlalchemy_credentials = self.get_sqlalchemy_credentials()
        aws_credentials = self.get_aws_credentials()
        
        # Connect to Redshift
        pr.connect_to_redshift(**sqlalchemy_credentials)

        # Connect to S3
        pr.connect_to_s3(
            **aws_credentials,
            bucket='dez2023-dez-prefect',
            subdirectory='test'
        )


    # Load DataFrame
    def load_to_redshift(self, data_frame: pd.DataFrame, table_name: str) -> None:
        pr.pandas_to_redshift(
            data_frame=data_frame,
            redshift_table_name=table_name,
            append=True
        )


    def close_connection(self):
        pr.close_up_shop()


@task(log_prints=True, retries=3)
def download_data(url: str, push_to_s3: bool) -> pd.DataFrame:
    # Extract filename from URL
    filename = url.split('/')[-1]

    # Download parquet file
    print(f"DOWNLOADING: {filename} from {url}")
    os.system('pwd')
    os.system(f"wget {url} -O {filename}")

    pf = pq.ParquetFile(filename)

    print(f"DOWNLOADED: {filename}")

    if push_to_s3:
        # Load AWS credentials from block
        aws_credentials_block = AwsCredentials.load('dez-credentials')

        bucket_name = 'dez2023-dez-prefect'

        s3_bucket = S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials_block,
        )

        # Specify parquet file
        s3_bucket_path = s3_bucket.upload_from_path(from_path=filename, to_path=filename)

        print(f"UPLOADED: {s3_bucket_path}/{filename}")

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
def ingest_data(parquet_file, batch_size, table, connector_block: SqlAlchemyConnector, credentials_block: AwsCredentials, url: str, export_to_s3: bool):
    counter = 0

    df = pd.DataFrame()

    aws_loader = AWSLoader(connector_block=connector_block, credentials_block=credentials_block)
    aws_loader.connect_to_redshift()

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
            aws_loader.load_to_redshift(data_frame=df_i, table_name=table)
            df = pd.concat([df, df_i])

        t_i_end = time()

        print(f"{batch_size} rows inserted in {round(t_i_end - t_i_start, 4)} seconds")

    aws_loader.close_connection()

    t_end = time()

    print(f"{counter} rows inserted in {round(t_end - t_start, 4)} seconds")

    if export_to_s3:
        bucket_name = 'dez2023-dez-prefect'
        filename = url.split('/')[-1].replace('.parquet', '_CLEANSED.parquet')

        df.to_parquet(filename)
        
        print(F"UPLOADING: {bucket_name}/{filename}")

        # Load AWS credentials from block
        aws_credentials_block = AwsCredentials.load('dez-credentials')

        s3_bucket = S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials_block,
        )

        # Specify parquet file
        s3_bucket.upload_from_path(from_path=filename, to_path=filename)


@flow(name='Subflow: Table Name')
def log_subflow(table_name: str) -> None:
    print(f"Logging subflow for {table_name}")


@flow(name='Flow: Ingest')
def main_flow():
    table = 'yellow_taxi_data'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

    log_subflow(table_name=table)

    parquet_file = download_data(url, push_to_s3=True)

    redshift_connector = SqlAlchemyConnector.load('redshift-connector')
    redshift_engine = redshift_connector.get_engine()

    create_table(redshift_engine, parquet_file, table=table)

    ingest_data(
        parquet_file=parquet_file,
        batch_size=100000,
        table=table,
        connector_block=SqlAlchemyConnector.load('redshift-connector'),
        credentials_block=AwsCredentials.load('dez-credentials'),
        url=url,
        export_to_s3=True
    )


if __name__ == '__main__':
    main_flow()
