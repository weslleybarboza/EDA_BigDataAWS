from pyspark.sql import SparkSession
from delta import *

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("TransformRawToStaging") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()
    
def process_data(spark):
  """Le um arquivo Parquet do S3 e escreve em formato Delta no S3."""

  # Caminhos dos arquivos (substitua pelos seus caminhos reais)
  source_path = "s3://eda-raw-zone-us-east-2-<Account ID>/DadosAbertos/Receita/"
  destination_path = "s3://eda-staging-zone-us-east-2-<Account ID>/DadosAbertos/Receita/"

  # Lendo o arquivo Parquet
  df = spark.read.parquet(source_path)

  # Escrevendo no formato Delta (substitua "overwrite" por "append" se preferir)
  df.write.format("delta").mode("overwrite").save(destination_path)

# # Executando a funcao de processamento
process_data(spark)

spark.stop()