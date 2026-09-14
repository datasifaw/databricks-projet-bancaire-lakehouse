from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =========================================================
# GOLD 1 - KPI GLOBAUX
# =========================================================

@dp.materialized_view(name="gold_kpi_global")
def gold_kpi_global():

    transactions = spark.read.table("silver_transactions")
    accounts = spark.read.table("silver_accounts")

    return (
        transactions
        .join(
            accounts.select("account_id", "customer_id"),
            on="account_id",
            how="inner"
        )
        .agg(
            F.count("*").alias("nombre_transactions"),
            F.round(F.sum("amount"), 2).alias("montant_total"),
            F.round(F.avg("amount"), 2).alias("montant_moyen"),
            F.countDistinct("customer_id").alias("nombre_clients_actifs")
        )
    )


# =========================================================
# GOLD 2 - TRANSACTIONS PAR TYPE
# =========================================================

@dp.materialized_view(name="gold_transactions_par_type")
def gold_transactions_par_type():

    df = spark.read.table("silver_transactions")

    return (
        df
        .groupBy("txn_type")
        .agg(
            F.count("*").alias("nombre_transactions"),
            F.round(F.sum("amount"), 2).alias("montant_total")
        )
        .orderBy(F.desc("montant_total"))
    )


# =========================================================
# GOLD 3 - TRANSACTIONS PAR CANAL
# =========================================================

@dp.materialized_view(name="gold_transactions_par_canal")
def gold_transactions_par_canal():

    df = spark.read.table("silver_transactions")

    return (
        df
        .groupBy("channel")
        .agg(
            F.count("*").alias("nombre_transactions"),
            F.round(F.sum("amount"), 2).alias("montant_total")
        )
        .orderBy(F.desc("montant_total"))
    )


# =========================================================
# GOLD 4 - TOP CLIENTS
# =========================================================

@dp.materialized_view(name="gold_top_clients")
def gold_top_clients():

    transactions = spark.read.table("silver_transactions")
    accounts = spark.read.table("silver_accounts")
    customers = spark.read.table("silver_customers")

    return (
        transactions
        .join(
            accounts.select("account_id", "customer_id"),
            on="account_id",
            how="inner"
        )
        .join(
            customers.select(
                "customer_id",
                "first_name",
                "last_name"
            ),
            on="customer_id",
            how="inner"
        )
        .groupBy(
            "customer_id",
            "first_name",
            "last_name"
        )
        .agg(
            F.count("*").alias("nombre_transactions"),
            F.round(F.sum("amount"), 2).alias("montant_total")
        )
        .orderBy(F.desc("montant_total"))
    )