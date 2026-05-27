from google.cloud import bigquery

from src.utils.logger import DEFAULT_LOGGER as logger

client = bigquery.Client()

for dataset in client.list_datasets():
    logger.info("Found BigQuery dataset: dataset_id=%s", dataset.dataset_id)
