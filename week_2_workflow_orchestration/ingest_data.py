from io import BytesIO
from time import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_aws import AwsCredentials, S3Bucket
import os
import pandas as pd
import pyarrow.parquet as pq
import boto3
import uuid


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
                'drivername': self.driver,
                'database': self.database,
                'host': self.host,
                'port': self.port,
                'username': self.user,
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

        global engine, connection

        url = URL.create(**sqlalchemy_credentials)
        
        # Configure Redshift engine; connect
        # https://docs.sqlalchemy.org/en/14/core/connections.html
        engine = create_engine(url)
        connection = engine.connect()


    def connect_to_s3(self):
        aws_credentials = self.get_aws_credentials()

        global s3_client

        s3_client = boto3.client(
            's3',
            **aws_credentials
        )


    # Load to S3
    def df_to_s3(self, data_frame: pd.DataFrame, bucket: str, object_name: str, subdir='.') -> None:
        if subdir == '.':
            key = object_name
        else:
            key = f"{subdir}/{object_name}"

        with BytesIO() as parquet_buffer:
            data_frame.to_parquet(parquet_buffer, index=False)
            s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())


    # Load DataFrame
    def load_to_redshift(self, conn, data_frame: pd.DataFrame, bucket: str, schema: str, table_name: str) -> None:
        identifier = uuid.uuid4()

        self.df_to_s3(
            data_frame=data_frame,
            bucket=bucket,
            subdir=schema,
            object_name=f"{table_name}_{identifier}.parquet"
        )

        print(f"LOADED {table_name}_{identifier}.parquet to S3")

        # https://docs.aws.amazon.com/redshift/latest/dg/loading-data-access-permissions.html
        # https://stackoverflow.com/questions/28271049/redshift-copy-operation-doesnt-work-in-sqlalchemy
        s = text(
            f'''
                COPY
                    {schema}.{table_name}

                FROM
                    's3://{bucket}/{schema}/{table_name}_{identifier}.parquet'

                IAM_ROLE
                    'arn:aws:iam::221807757711:role/s3_readonly'

                PARQUET;
            '''
        ).execution_options(autocommit=True)

        print(f"EXECUTING {s}")
        conn.execute(s)


    def close_connection(self) -> None:
        connection.close()


@task(log_prints=True, retries=3)
def download_data(url: str, push_to_s3=False, **kwargs) -> pd.DataFrame:
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

        s3_bucket = S3Bucket(
            bucket_name=kwargs['bucket'],
            credentials=aws_credentials_block,
        )

        # Specify parquet file
        s3_bucket_path = s3_bucket.upload_from_path(from_path=filename, to_path=filename)

        print(f"UPLOADED: {s3_bucket_path}/{filename}")

    return pf


# @task(log_prints=True, retries=3)
def create_table(conn, schema, table) -> None:
    # Define CREATE statement
    s = f'''
        CREATE TABLE {schema}.{table} (
            vendorid                BIGINT,
            tpep_pickup_datetime    TIMESTAMP,
            tpep_dropoff_datetime   TIMESTAMP,
            passenger_count         DOUBLE PRECISION,
            trip_distance           DOUBLE PRECISION,
            ratecodeid              DOUBLE PRECISION,
            store_and_fwd_flag      VARCHAR(256),
            pulocationid            BIGINT,
            dolocationid            BIGINT,
            payment_type            BIGINT,
            fare_amount             DOUBLE PRECISION,
            extra                   DOUBLE PRECISION,
            mta_tax                 DOUBLE PRECISION,
            tip_amount              DOUBLE PRECISION,
            tolls_amount            DOUBLE PRECISION,
            improvement_surcharge   DOUBLE PRECISION,
            total_amount            DOUBLE PRECISION,
            congestion_surcharge    DOUBLE PRECISION,
            airport_fee             DOUBLE PRECISION
        );
    '''

    # Use AWSLoader connection to execute DROP STATEMENT (if exists); execute above CREATE statement
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    conn.execute(s)


@task(log_prints=True, retries=3)
def ingest_data(parquet_file, batch_size, bucket, schema, table,
                connector_block: SqlAlchemyConnector, credentials_block: AwsCredentials,
                url: str, export_to_s3: bool):
    counter = 0

    df = pd.DataFrame()

    aws_loader = AWSLoader(connector_block=connector_block, credentials_block=credentials_block)

    aws_loader.connect_to_s3()
    aws_loader.connect_to_redshift()

    create_table(conn=connection, schema=schema, table=table)

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
            df_i['store_and_fwd_flag'] = df_i['store_and_fwd_flag'].fillna(value='')
            df_i['tpep_pickup_datetime'] = pd.to_datetime(df_i['tpep_pickup_datetime'])
            df_i['tpep_dropoff_datetime'] = pd.to_datetime(df_i['tpep_dropoff_datetime'])
        finally:
            counter += len(df_i)
            aws_loader.load_to_redshift(conn=connection, data_frame=df_i, bucket=bucket, schema=schema, table_name=table)
            df = pd.concat([df, df_i])

        t_i_end = time()

        print(f"{batch_size} rows inserted in {round(t_i_end - t_i_start, 4)} seconds")

    aws_loader.close_connection()

    t_end = time()

    print(f"{counter} rows inserted in {round(t_end - t_start, 4)} seconds")

    if export_to_s3:
        bucket_name = bucket
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
def log_subflow(schema: str, table: str) -> None:
    print(f"Logging subflow for {schema}.{table}")


@flow(name='Flow: Ingest')
def main_flow(bucket: str, schema: str, table: str, url: str) -> None:
    log_subflow(schema=schema, table=table)

    parquet_file = download_data(url, push_to_s3=True, bucket=bucket)

    ingest_data(
        parquet_file=parquet_file,
        batch_size=100000,
        bucket=bucket,
        schema=schema,
        table=table,
        connector_block=SqlAlchemyConnector.load('redshift-connector'),
        credentials_block=AwsCredentials.load('dez-credentials'),
        url=url,
        export_to_s3=False
    )


if __name__ == '__main__':
    BUCKET = 'dez2023-dez-prefect'
    SCHEMA = 'ny_taxi'
    TABLE = 'yellow_taxi_data'
    SRC_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

    main_flow(bucket=BUCKET, schema=SCHEMA, table=TABLE, url=SRC_URL)
