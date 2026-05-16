"""Airflow tasks for air quality data extraction."""

from datetime import datetime
from pathlib import Path
from typing import Optional

from airflow.models import BaseOperator


class ExtractOpenDOSMOperator(BaseOperator):
    """Airflow operator to extract data from OpenDOSM."""

    ui_color = "#4CAF50"

    def __init__(
        self,
        bronze_path: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bronze_path = bronze_path

    def execute(self, context):
        from src.extract.opendosm_extract import extract_opendosm

        self.log.info("Starting OpenDOSM data extraction")
        filepath = extract_opendosm()
        self.log.info(f"OpenDOSM data saved to {filepath}")
        return str(filepath)


class ExtractOpenAQOperator(BaseOperator):
    """Airflow operator to extract data from OpenAQ."""

    ui_color = "#2196F3"

    def __init__(
        self,
        bronze_path: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bronze_path = bronze_path

    def execute(self, context):
        from src.extract.openaq_extract import extract_openaq

        self.log.info("Starting OpenAQ data extraction")
        locations_file, measurements_file = extract_openaq()
        self.log.info(f"OpenAQ data saved to {locations_file} and {measurements_file}")
        return str(locations_file), str(measurements_file)


class IngestToSilverOperator(BaseOperator):
    """Airflow operator to ingest bronze data to silver layer."""

    ui_color = "#FF9800"

    def __init__(
        self,
        source: str,
        bronze_file: str,
        silver_path: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source = source
        self.bronze_file = bronze_file
        self.silver_path = silver_path

    def execute(self, context):
        from src.extract.spark_ingest import (
            ingest_openaq_bronze_to_silver,
            ingest_opendosm_bronze_to_silver,
        )

        self.log.info(f"Ingesting {self.source} data from bronze to silver")

        if self.source == "opendosm":
            df = ingest_opendosm_bronze_to_silver(self.bronze_file, self.silver_path)
        elif self.source == "openaq":
            df = ingest_openaq_bronze_to_silver(self.bronze_file, self.silver_path)
        else:
            raise ValueError(f"Unknown source: {self.source}")

        self.log.info(f"Ingestion complete: {df.count()} records")
        return True


def create_extraction_dag():
    """Create the main extraction DAG."""
    from airflow import DAG

    dag = DAG(
        "air_quality_extraction",
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "start_date": datetime(2024, 1, 1),
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 3,
        },
        description="Extract air quality data from OpenDOSM and OpenAQ",
        schedule_interval="@daily",
        catchup=False,
    )

    return dag