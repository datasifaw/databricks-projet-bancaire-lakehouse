from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, input_file_name

BASE_PATH = "/Volumes/workspace/banque_db/sqlserver_files/"


def lire_csv(nom_fichier):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(BASE_PATH + nom_fichier)
        .withColumn("date_ingestion", current_timestamp())
        .withColumn("fichier_source", input_file_name())
    )


@dp.materialized_view(name="bronze_branches")
def bronze_branches():
    return lire_csv("branches.csv")


@dp.materialized_view(name="bronze_customers")
def bronze_customers():
    return lire_csv("customers.csv")


@dp.materialized_view(name="bronze_accounts")
def bronze_accounts():
    return lire_csv("accounts.csv")


@dp.materialized_view(name="bronze_transactions")
def bronze_transactions():
    return lire_csv("transactions.csv")