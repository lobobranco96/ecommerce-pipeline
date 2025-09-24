import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import List
import logging 
import json
import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

class MinioUtils:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        
        self.s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
                )
        today = datetime.today()
        self.year = today.strftime("%Y")
        self.month = today.strftime("%m")
        self.day = today.strftime("%d")


    def upload_df_as_parquet(self, df, dataset_name, bucket_name):
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

        key = f"{dataset_name}/year={self.year}/month={self.month}/day={self.day}/{dataset_name}.parquet"

        # Upload no MinIO
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

        logger.info(f"Upload concluído: s3://{bucket_name}/{key}")
        return f"s3a://{bucket_name}/{key}"

    def list_raw_objects(self) -> List[str]:
      response = self.s3_client.list_objects_v2(Bucket="raw", Prefix="")
      files = [
          obj['Key'] for obj in response.get('Contents', [])
          if f"year={self.year}/month={self.month}/day={self.day}" in obj['Key']
      ]

      for f in files:
          logger.info(f"Arquivo do dia de hoje encontrado: {f}")

      if not files:
          logger.warning("Nenhum arquivo do dia de hoje foi encontrado no bucket 'raw'.")

      return files

    def list_processed_objects(self) -> List[str]:
      response = self.s3_client.list_objects_v2(Bucket="processed", Prefix="")
      files = [
          obj['Key'] for obj in response.get('Contents', [])
          if f"year={self.year}/month={self.month}/day={self.day}" in obj['Key']
      ]

      for f in files:
          logger.info(f"Arquivo do dia de hoje encontrado: {f}")

      if not files:
          logger.warning("Nenhum arquivo do dia de hoje foi encontrado no bucket 'processed'.")

      return files

    def object_validation(self, table):
      file_path = f"{table}/year={self.year}/month={self.month}/day={self.day}/*.json"
      obj = self.s3_client.get_object(Bucket="processed", Key=file_path)
      result = json.loads(obj["Body"].read())
      return result