from pyspark.sql import SparkSession



def do_datelist_transformation(spark, dataframe):
    query = """
        with a as (
            select * 
            from users_cumulated
            where date = '2023-01-30'
        ),
        b as (
            select 
            explode(sequence(to_date('2023-01-01'), to_date('2023-01-30'), interval '1 day')) as generate_series
        ),
        c as (
            select
                *,
                case
                    when array_contains(dates_active, generate_series)
                        then pow(2, 32 - cast((date - generate_series) as int) -1)
                    else 0
                end as datelist_pow2
            from a 
            cross join b
        ),
        d as (
            select 
                user_id,
                min(date) as date,
                min(dates_active) as dates_active, -- just for referece, we would not actually keep this one
                lpad(bin(sum(datelist_pow2)), 32, 0) as datelist
            from c
            group by user_id
        )
        select * from d
    """
    dataframe.createOrReplaceTempView("users_cumulated")
    return spark.sql(query)

def main():
    spark = (SparkSession
             .builder()
             .master("local")
             .appName("datelist")
             .getOrCreate())
    df = do_datelist_transformation(spark, spark.table("users_cumulated")) 
    df.write.mode("overwrite").insertInto("user_activity_datelist")