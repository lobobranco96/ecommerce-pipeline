from typing import List
from datetime import datetime
import logging 

logger = logging.getLogger(__name__)

def list_raw_objects(s3_client) -> List(str):
  today = datetime.today()
  year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

  response = s3_client.list_objects_v2(Bucket="raw", Prefix="")
  files = [
      obj['Key'] for obj in response.get('Contents', [])
      if f"year={year}/month={month}/day={day}" in obj['Key']
  ]

  for f in files:
      logger.info(f"Arquivo do dia de hoje encontrado: {f}")

  if not files:
      logger.warning("Nenhum arquivo do dia de hoje foi encontrado no bucket 'raw'.")

  return files[0]