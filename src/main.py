## Your PySpark code.
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd

def setup_creds():
    """
    Usually secrets are kept in the hidden files. For the sake of completing projects quickly we are using
    aws's default key export format csv
    """ 
    creds = pd.read_csv("/app/rootkey.csv")
    creds = creds["creds"].str.split("=", expand = True)

    ACCESS_KEY=creds[1][0]
    SECRET_KEY=creds[1][1]

    return ACCESS_KEY, SECRET_KEY

def setup_spark_session():
    appName = "Rippleshot"
    master = "local"

    ACCESS_KEY, SECRET_KEY = setup_creds()

    conf = SparkConf()
    conf.set('spark.hadoop.fs.s3a.access.key', ACCESS_KEY)
    conf.set('spark.hadoop.fs.s3a.secret.key', SECRET_KEY)

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .master(master) \
        .appName(appName) \
        .getOrCreate()

    return spark

def de_dup(df):
    """
    This will drop records with same values in all of the columns. If there is complex logic involved 
    for identifying duplicates then this function can be amended.
    """
    return df.drop_duplicates()

def check_latest_account(df):
    """
    latest accounts = most recent updated date. So select record with most recent updated_date per id.
    """
    w = Window.partitionBy('account_id').orderBy(desc('updated_date'))
    df = df.withColumn('Rank',dense_rank().over(w))

    return df.filter(df.Rank == 1).drop(df.Rank)
    
def ingest_accounts(spark, BUCKETNAME):
    """
    This function loads accounts.tsv. de-dups it and uses write.partitionBy method to save latest acount details to s3
    partitioned by year, month & day in parquet format
    """
    path = "/app/client_data_files/accounts.tsv"
    accounts = spark.read.csv(path, sep=r'\t', header=True)

    accounts = de_dup(accounts)

    accounts = accounts.withColumn("timestamp", to_timestamp(col("creation_date"), 'M/d/y H:m:s')) \
            .withColumn("year", date_format(col("timestamp"), "yyyy")) \
            .withColumn("month", date_format(col("timestamp"), "MM")) \
            .withColumn("day", date_format(col("timestamp"), "dd"))

    accounts = check_latest_account(accounts)

    accounts.write.partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .option("header", "true").option("sep", "\t") \
        .parquet("s3a://{}/accounts/".format(BUCKETNAME))
    
    return accounts

def ingest_auths(spark, BUCKETNAME):
    """
    This function loads auths.tsv. de-dups it and uses write.partitionBy method to save auths details to s3
    partitioned by year, month & day in parquet format
    """
    path = "/app/client_data_files/auths.csv"
    auths = spark.read.csv(path, header=True)

    # Cleaning empty columns from auths.csv
    temp = list(auths.columns)
    temp.remove("_c0")
    temp.remove("_c2")
    for column in temp:
        if " " in column:
            auths = auths.withColumnRenamed(column, column.replace(" ", "_"))
            temp[temp.index(column)] = column.replace(" ", "_")
    auths = auths.select(temp)

    auths = de_dup(auths)

    auths = auths.withColumn("timestamp", to_timestamp(col("Transmit_Time"), 'M/d/y H:m:s')) \
            .withColumn("year", date_format(col("timestamp"), "yyyy")) \
            .withColumn("month", date_format(col("timestamp"), "MM")) \
            .withColumn("day", date_format(col("timestamp"), "dd"))

    auths.write.partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .option("header", "true").option("sep", ",") \
        .parquet("s3a://{}/auths/".format(BUCKETNAME))

    return auths

def generate_reports(accounts, auths):
    combined = accounts.join(auths, accounts.account_id ==  auths.account_id, "inner").drop(auths.account_id).drop(accounts.year).drop(accounts.month).drop(accounts.day)

    # Report 1 - how many transactions happened per account first name and last name, per day
    combined.groupBy("account_id", "first_name", "last_name", "year", "month", "day").agg(count("account_id").alias("transactions_per_day_per_account")) \
        .toPandas().to_csv("/app/reports/transactions_per_day_per_account.csv", index=False)
    
    # Report 2 - top 3 merchants by dollars spent
    combined.groupBy("merchant_name").agg(sum("Amount").alias("dollors_spent")).orderBy(desc("dollors_spent")) \
        .toPandas().head(3).to_csv("/app/reports/top_3_dollors_spent.csv", index=False)


if __name__ == "__main__":
    BUCKETNAME = "rippleshot"
    spark = setup_spark_session()
    accounts = ingest_accounts(spark, BUCKETNAME)
    auths = ingest_auths(spark, BUCKETNAME)
    generate_reports(accounts, auths)
