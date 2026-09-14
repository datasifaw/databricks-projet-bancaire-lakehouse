from pyspark import pipelines as dp
from pyspark.sql import functions as F


def nettoyer_textes(df):
    for nom_colonne, type_colonne in df.dtypes:
        if type_colonne == "string":
            df = df.withColumn(
                nom_colonne,
                F.trim(F.col(nom_colonne))
            )
    return df


# =========================================================
# SILVER BRANCHES
# =========================================================

@dp.materialized_view(name="silver_branches")
@dp.expect("branch_code_non_null", "branch_code IS NOT NULL")
def silver_branches():

    df = spark.read.table("bronze_branches")

    df = nettoyer_textes(df)

    return df.dropDuplicates(["branch_code"])


# =========================================================
# SILVER CUSTOMERS
# =========================================================

@dp.materialized_view(name="silver_customers")
@dp.expect("customer_id_non_null", "customer_id IS NOT NULL")
@dp.expect("branch_code_non_null", "branch_code IS NOT NULL")
def silver_customers():

    df = spark.read.table("bronze_customers")

    df = nettoyer_textes(df)

    df = (
        df
        .withColumn(
            "customer_id",
            F.expr("try_cast(customer_id AS INT)")
        )
        .withColumn(
            "date_of_birth",
            F.expr("try_cast(date_of_birth AS DATE)")
        )
        .withColumn(
            "kyc_status",
            F.upper(F.col("kyc_status"))
        )
    )

    return (
        df
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


# =========================================================
# SILVER ACCOUNTS
# =========================================================

@dp.materialized_view(name="silver_accounts")
@dp.expect("account_id_non_null", "account_id IS NOT NULL")
@dp.expect("customer_id_non_null", "customer_id IS NOT NULL")
@dp.expect("balance_non_negative", "balance >= 0")
def silver_accounts():

    df = spark.read.table("bronze_accounts")

    df = nettoyer_textes(df)

    df = (
        df
        .withColumn(
            "account_id",
            F.expr("try_cast(account_id AS BIGINT)")
        )
        .withColumn(
            "customer_id",
            F.expr("try_cast(customer_id AS INT)")
        )
        .withColumn(
            "balance",
            F.expr("try_cast(balance AS DOUBLE)")
        )
        .withColumn(
            "opened_date",
            F.expr("try_cast(opened_date AS DATE)")
        )
        .withColumn(
            "account_type",
            F.upper(F.col("account_type"))
        )
        .withColumn(
            "status",
            F.upper(F.col("status"))
        )
        .withColumn(
            "currency",
            F.upper(F.col("currency"))
        )
    )

    return (
        df
        .filter(
            F.col("account_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("balance").isNotNull()
        )
        .dropDuplicates(["account_id"])
    )


# =========================================================
# SILVER TRANSACTIONS
# =========================================================

@dp.materialized_view(name="silver_transactions")
@dp.expect("txn_id_non_null", "txn_id IS NOT NULL")
@dp.expect("account_id_non_null", "account_id IS NOT NULL")
@dp.expect("amount_positive", "amount > 0")
def silver_transactions():

    df = spark.read.table("bronze_transactions")

    df = nettoyer_textes(df)

    df = (
        df
        .withColumn(
            "txn_id",
            F.expr("try_cast(txn_id AS BIGINT)")
        )
        .withColumn(
            "account_id",
            F.expr("try_cast(account_id AS BIGINT)")
        )
        .withColumn(
            "amount",
            F.expr("try_cast(amount AS DOUBLE)")
        )
        .withColumn(
            "txn_timestamp",
            F.expr("try_cast(txn_timestamp AS TIMESTAMP)")
        )
        .withColumn(
            "txn_type",
            F.upper(F.col("txn_type"))
        )
        .withColumn(
            "channel",
            F.upper(F.col("channel"))
        )
        .withColumn(
            "status",
            F.upper(F.col("status"))
        )
    )

    return (
        df
        .filter(
            F.col("txn_id").isNotNull()
            & F.col("account_id").isNotNull()
            & F.col("amount").isNotNull()
        )
        .dropDuplicates(["txn_id"])
    )