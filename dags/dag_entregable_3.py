import json
import logging
from datetime import datetime, timedelta
from http import HTTPStatus

import boto3
import pyspark.sql.functions as F
import redshift_connector
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def save_in_s3(bucket: str, key: str, data: str):
    client = boto3.client(
        "s3",
        endpoint_url=Variable.get("S3_ENDPOINT_URL"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client.put_object(Bucket=bucket, Key=key, Body=data)


def read_from_s3(bucket: str, key: str) -> str:
    client = boto3.client(
        "s3",
        endpoint_url=Variable.get("S3_ENDPOINT_URL"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def get_spark_session():
    spark = (
        SparkSession.builder.master("spark://spark:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-hadoop-cloud_2.12:3.2.0,org.postgresql:postgresql:42.6.0",
        )
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endpoint", Variable.get("S3_ENDPOINT_URL"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark


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


@dag(
    dag_id="entregable_3",
    schedule=None,
    start_date=datetime(2023, 5, 30),
    dagrun_timeout=timedelta(minutes=30),
    tags=["coder-entregables"],
)
def entregable_3():
    """
    ### Entregable 3 DAG
    Este pipeline extrae datos de distintos modelos de motos desde
    motorcycles API, realiza algunas transformaciones simples y
    los carga en Redshift.

    En los pasos intermedios utiliza S3 para guardar los datos y
    Spark para transformarlos y cargarlos en Redshift.
    """

    @task
    def create_s3_bucket() -> str:
        """
        Esta tarea es idempotente. Primero revisa que el bucket no exista,
        si no existe lo crea pero si existe no hace nada.
        """

        client = boto3.client(
            "s3",
            endpoint_url=Variable.get("S3_ENDPOINT_URL"),
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3_bucket = "entregable-3"
        logger.info(f"Attempting to create bucket {s3_bucket}...")

        try:
            _ = client.head_bucket(Bucket=s3_bucket)
            logger.info(f"Bucket {s3_bucket} already exists! Leaving it untouched.")
        except Exception:
            client.create_bucket(Bucket=s3_bucket)

        logger.info("Done.")
        return s3_bucket

    @task
    def get_motorcycles_data(s3_bucket: str) -> dict[str, str]:
        """
        Extrae datos desde [motorcycles API](https://www.api-ninjas.com/api/motorcycles)
        para diferentes fabricantes y los guarda en formato JSON en S3.
        """

        key = "data.json"
        logger.info("Checking if data already exists in S3...")
        motorcycles_data = json.loads(read_from_s3(s3_bucket, key))
        if not motorcycles_data:
            logger.info("Reading motorcycles data from API...")
            manufacturers = [
                "Motomel",
                "Zanella",
                "Honda",
                "Kawasaki",
                "Harley-Davidson",
            ]
            years = list(range(2015, 2023))

            motorcycles_data = ETL.extract(manufacturers, years)

        motorcycles_data_string = json.dumps(motorcycles_data)
        logger.info(f"Loading data in S3 s3://{s3_bucket}/{key}")
        save_in_s3(s3_bucket, key, motorcycles_data_string)

        return {"s3_bucket": s3_bucket, "key": key}

    @task
    def transform_motorcycles_data_with_spark(extract_response: dict) -> dict[str, str]:
        """
        Consume los datos de la task anterior con Spark, los transforma y
        los guarda como JSON en S3. Los datos transformados no
        sobrescriben a los originales, sino que se guardan como un
        archivo distinto.
        """

        logger.info("Loading data from the extract step...")
        s3_bucket = extract_response["s3_bucket"]
        s3_key = extract_response["key"]
        raw_motorcycles_data = json.loads(read_from_s3(s3_bucket, s3_key))

        # Create Spark DataFrame.
        logger.info("Creating spark session...")
        spark = get_spark_session()

        logger.info("Building DataFrame from data...")
        df = spark.createDataFrame(raw_motorcycles_data)

        logger.info("Applying transformations...")
        transformed_df = ETL.transform(df)

        # Save transformed data as json in S3.
        new_key = "transformed-data"
        logger.info(
            f"Saving transformed data in S3 bucket {s3_bucket} with key {new_key}..."
        )
        transformed_df.write.json(f"s3a://{s3_bucket}/{new_key}", mode="overwrite")

        logger.info("Done.")

        return {"s3_bucket": s3_bucket, "key": new_key}

    @task
    def create_redshift_table() -> str:
        """
        Crea la tabla 'motorcycles_entregable3' en Redshift solamente
        si dicha tabla no fue creada en una corrida anterior.
        """

        logger.info("Connecting to Redshift...")
        connection = redshift_connector.connect(
            host=Variable.get("REDSHIFT_CODER_HOST"),
            database=Variable.get("REDSHIFT_CODER_DB"),
            port=int(Variable.get("REDSHIFT_CODER_PORT")),
            user=Variable.get("REDSHIFT_CODER_USER"),
            password=Variable.get("REDSHIFT_CODER_PASSWORD"),
        )
        connection.autocommit = True
        cursor = connection.cursor()

        table_name = "motorcycles_entregable3"
        logger.info(f"Creating table {table_name}...")
        schema_table = f"{Variable.get('REDSHIFT_CODER_SCHEMA')}.{table_name}"
        statement = f"""
        CREATE TABLE IF NOT EXISTS {schema_table} (
            make varchar not null,
            model varchar not null,
            year integer not null,
            type varchar not null,
            bore_stroke varchar,
            clutch varchar,
            compression varchar,
            cooling varchar,
            displacement varchar,
            dry_weight varchar,
            emission varchar,
            engine varchar,
            frame varchar,
            front_brakes varchar,
            front_suspension varchar,
            front_tire varchar,
            front_wheel_travel varchar,
            fuel_capacity varchar,
            fuel_consumption varchar,
            fuel_control varchar,
            fuel_system varchar,
            gearbox varchar,
            ground_clearance varchar,
            ignition varchar,
            lubrication varchar,
            power varchar,
            rear_brakes varchar,
            rear_suspension varchar,
            rear_tire varchar,
            rear_wheel_travel varchar,
            seat_height varchar,
            starter varchar,
            top_speed varchar,
            torque varchar,
            total_height varchar,
            total_length varchar,
            total_weight varchar,
            total_width varchar,
            transmission varchar,
            valves_per_cylinder varchar,
            wheelbase varchar,
            dry_weight_kg float,
            total_weight_kg float
        )
        diststyle even
        sortkey(year);
        """
        cursor.execute(statement)
        logger.info("Table created successfully.")

        return schema_table

    @task
    def load_motorcycles_data_in_redshift(
        transform_response: dict[str, str], table: str
    ):
        """
        Guarda los datos resultantes de la transformación en Redshift.
        Para ello, lee los datos desde S3 con Spark.
        """
        logger.info("Getting Spark session...")
        spark = get_spark_session()

        s3_bucket = transform_response["s3_bucket"]
        s3_key = transform_response["key"]
        logger.info(f"Reading data from S3 s3://{s3_bucket}/{s3_key}")
        df = spark.read.json(f"s3a://{s3_bucket}/{s3_key}")

        logger.info(f"Loading data in Redshift table {table}...")
        ETL.load(df, table)

    table = create_redshift_table()
    s3_bucket = create_s3_bucket()
    get_motorcycles_data_response = get_motorcycles_data(s3_bucket)
    transform_motorcycles_data_response = transform_motorcycles_data_with_spark(
        get_motorcycles_data_response
    )
    load_motorcycles_data_in_redshift(transform_motorcycles_data_response, table)


entregable_3()
