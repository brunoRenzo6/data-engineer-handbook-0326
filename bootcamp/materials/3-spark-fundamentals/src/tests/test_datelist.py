from chispa.dataframe_comparer import *

from ..jobs.datelist_job import do_datelist_transformation

from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import date

def test_datelist(spark):
    schema_users_cumulated = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("dates_active", ArrayType(DateType()), True)
    ])
    df_users_cumulated = spark.createDataFrame(
        [
            Row(user_id = 1,
                date = date(2023, 1, 30),
                dates_active = [date(2023, 1, 30), date(2023, 1, 28), date(2023, 1, 1)]),
        ],
        schema=schema_users_cumulated
    )

    df_actual = do_datelist_transformation(spark, df_users_cumulated)

    schema_expected = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("dates_active", ArrayType(DateType()), True),
        StructField("datelist", StringType(), True)
    ])
    df_expected = spark.createDataFrame(
        [
            Row(user_id = 1,
                date = date(2023, 1, 30),
                dates_active = [date(2023, 1, 30), date(2023, 1, 28), date(2023, 1, 1)],
                datelist = '10100000000000000000000000000100',
            )
        ],
        schema=schema_expected
    )

    assert_df_equality(df_actual, df_expected)