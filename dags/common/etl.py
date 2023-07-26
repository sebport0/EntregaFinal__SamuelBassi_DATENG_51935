import logging
from http import HTTPStatus

import pyspark.sql.functions as F
import requests
from airflow.models import Variable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ETL:
    @staticmethod
    def extract(manufacturers: list[str], years: list[int]) -> list[dict]:
        api_url = Variable.get("MOTORCYCLES_API_URL")
        api_token = Variable.get("MOTORCYCLES_API_KEY")
        headers = {"X-Api-Key": api_token}

        logger.info("Requesting raw motorcycle data...")
        raw_motorcycles_data = []
        for manufacturer in manufacturers:
            for year in years:
                logger.info(f"Manufacturer {manufacturer}, year {year}.")
                params = {"make": manufacturer, "year": year}
                response = requests.get(api_url, params=params, headers=headers)

                if response.status_code != HTTPStatus.OK:
                    logger.error(
                        f"Something when wrong when requesting data from {manufacturer} in year {year}"
                    )
                    pass

                raw_data = response.json()
                # There might not be any manufacturer information for the given year.
                # Do not append empty lists to the final result.
                if raw_data:
                    raw_motorcycles_data.append(response.json())

        # Flatten the extracted data because each one of the API responses is a list[dict].
        motorcycles_data = [
            motorcycle
            for raw_data_sublist in raw_motorcycles_data
            for motorcycle in raw_data_sublist
        ]

        return motorcycles_data

    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        transformed_df = df.withColumn("year", col("year").cast("Integer"))

        def weight_in_kg(value):
            if value:
                return float(value.split(" kg")[0])
            return None

        udf_weight_in_kg = F.udf(weight_in_kg, FloatType())
        for column in ["dry_weight", "total_weight"]:
            transformed_df = transformed_df.withColumn(
                f"{column}_kg", udf_weight_in_kg(column)
            )

        return transformed_df

    @staticmethod
    def load(df: DataFrame, table: str):
        db_url = f"jdbc:postgresql://{Variable.get('REDSHIFT_CODER_HOST')}:{Variable.get('REDSHIFT_CODER_PORT')}/{Variable.get('REDSHIFT_CODER_DB')}"
        user = Variable.get("REDSHIFT_CODER_USER")
        password = Variable.get("REDSHIFT_CODER_PASSWORD")
        _ = (
            df.write.format("jdbc")
            .option("url", db_url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
