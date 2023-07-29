import json
import logging
import smtplib
from datetime import datetime, timedelta

import boto3
import redshift_connector
from airflow.decorators import dag, task
from airflow.models import Variable
from common.common import get_spark_session, read_from_s3, save_in_s3
from common.etl import ETL

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dag(
    dag_id="entregable_final",
    schedule=None,
    start_date=datetime(2023, 5, 30),
    dagrun_timeout=timedelta(minutes=30),
    tags=["coder-entregables"],
)
def entregable_final():
    """
    ### Entregable Final DAG
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
        s3_bucket = "entregable-final"
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
        try:
            motorcycles_data = json.loads(read_from_s3(s3_bucket, key))
        except Exception:
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
        Crea la tabla 'motorcycles_final' en Redshift solamente
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

        table_name = "motorcycles_final"
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

    table_name = create_redshift_table()
    s3_bucket_name = create_s3_bucket()
    raw_s3_location = get_motorcycles_data(s3_bucket_name)
    transformed_s3_location = transform_motorcycles_data_with_spark(raw_s3_location)
    load_motorcycles_data_in_redshift(transformed_s3_location, table_name)


entregable_final()
