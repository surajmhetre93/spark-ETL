import pandas as pd
import pytest
import sys

import findspark
findspark.init()
from pyspark.sql import SparkSession

sys.path.append('/app/src/')
import main as m

@pytest.fixture()
def create_df():
    appName = "Rippleshot"
    master = "local"
    spark = SparkSession \
        .builder \
        .master(master) \
        .appName(appName) \
        .getOrCreate()
    accounts = spark.read.csv("/app/tests/test_data/accounts.tsv", sep=r'\t', header=True)
    accounts = accounts.drop_duplicates()

    return accounts

def test_check_latest_account(create_df):
    
    returned_df = m.check_latest_account(create_df).toPandas()
    expected_df = pd.read_csv('/app/tests/test_data/latest_accounts.csv')

    pd.testing.assert_frame_equal(returned_df[["account_id", "updated_date"]], expected_df[["account_id", "updated_date"]])
