import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import List
import logging 
import json

logger = logging.getLogger(__name__)

class MinioUtils:
    def __init__(self, s3_client):
        self.s3_client = s3_client
        today = datetime.today()
        self.year = today.strftime("%Y")
        self.month = today.strftime("%m")
        self.day = today.strftime("%d")


    def upload_df_as_parquet(self, df, dataset_name, bucket_name, partition_cols=None):
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

    def list_raw_objects(self) -> List[str]:
     # year, month, day = self.today.strftime("%Y"), self.today.strftime("%m"), self.today.strftime("%d")

      response = self.s3_client.list_objects_v2(Bucket="raw", Prefix="")
      files = [
          obj['Key'] for obj in response.get('Contents', [])
          if f"year={self.year}/month={self.month}/day={self.day}" in obj['Key']
      ]

      for f in files:
          logger.info(f"Arquivo do dia de hoje encontrado: {f}")

      if not files:
          logger.warning("Nenhum arquivo do dia de hoje foi encontrado no bucket 'raw'.")

      return files[0]

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

      return files[0]

    def object_validation(self, table):
      file_path = f"{table}/year={self.year}/month={self.month}/day={self.day}/*.json"
      obj = self.s3_client.get_object(Bucket="processed", Key=file_path)
      result = json.loads(obj["Body"].read())
      return result