import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

class MinioUploader:
    def __init__(self, endpoint_url, access_key, secret_key, bucket_name):
        self.bucket = bucket_name
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

    def upload_df_as_parquet(self, df, dataset_name, partition_cols=None):
        today = datetime.today()
        year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

        # Converter Pandas para Arrow Table com schema otimizado
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Buffer Parquet otimizado
        parquet_buffer = io.BytesIO()
        pq.write_table(
            table,
            parquet_buffer,
            compression="snappy",      
            use_dictionary=True,          
            coerce_timestamps="ms",      
            data_page_size=64 * 1024      
        )
        parquet_buffer.seek(0)

        key = f"{dataset_name}/year={year}/month={month}/day={day}/{dataset_name}.parquet"

        # Upload no MinIO
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

        print(f"Upload concluído: s3://{self.bucket}/{key}")
