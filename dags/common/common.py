import boto3
from airflow.models import Variable
from pyspark.sql import SparkSession


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
