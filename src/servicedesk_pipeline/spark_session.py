"""Construção da SparkSession, com Delta Lake e acesso a S3 opcionais.

O mesmo builder serve três ambientes:
  * local / CI com ``table_format=parquet`` -> Spark puro, sem JAR extra;
  * local / produção com ``table_format=delta`` -> habilita a extensão Delta;
  * ``backend=s3`` -> configura o conector s3a (AWS).
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Settings


def build_spark(settings: Settings) -> SparkSession:
    """Cria (ou recupera) a SparkSession conforme a configuração."""
    builder = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.spark_master)
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.enabled", "false")
    )

    use_delta = settings.table_format == "delta"
    if use_delta:
        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    if settings.backend == "s3":
        # Conector s3a. As credenciais vêm da cadeia padrão da AWS
        # (variáveis de ambiente, perfil, IAM role no Databricks/EMR).
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.s3_region}.amazonaws.com")
        )

    if use_delta:
        # configure_spark_with_delta_pip baixa os JARs do Delta compatíveis
        # com a versão do PySpark instalada (precisa de acesso ao Maven Central).
        from delta import configure_spark_with_delta_pip

        extra_packages = []
        if settings.backend == "s3":
            extra_packages.append("org.apache.hadoop:hadoop-aws:3.4.1")
        builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
