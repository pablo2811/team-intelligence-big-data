from datetime import datetime, timedelta
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import happybase
from pyspark.sql import SQLContext


def load_hive_table(spark, table_name):
    hive_context = HiveContext(spark.sparkContext)
    t = hive_context.table(table_name)
    return t


def main():
    spark = (SparkSession.builder.enableHiveSupport().appName("Month Data Aggregation").getOrCreate())
    date = (datetime.now().date() - timedelta(1)).strftime("%Y-%m")

    weather_table = load_hive_table(spark, table_name='weather')
    fire_table = load_hive_table(spark, table_name='fire')
    weather_table = weather_table.filter(F.col('partition_dt').contains(date))
    fire_table = fire_table.filter(F.col('partition_dt').contains(date)).withColumn(
        "hour", F.expr("""CONCAT(hour, ':00:00')"""))
    fire_table = fire_table
    fire_weather = fire_table.join(weather_table, (fire_table["partition_dt"] == weather_table["partition_dt"]) &
                                   (fire_table["hour"] == weather_table["datetime"])).select(
                                       "type", "day", "hour", "temp", "humidity", "windgust", "pressure", "visibility")
    columns_to_aggregate = fire_weather.columns[3:]
    agg_type_day = fire_weather.groupBy(["type", "day"]).agg(
        F.count("*").alias("count"),
        *[F.max(F.col(x)).alias("max_" + x) for x in columns_to_aggregate],
        *[F.min(F.col(x)).alias("min_" + x) for x in columns_to_aggregate],
        *[F.avg(F.col(x)).alias("avg_" + x) for x in columns_to_aggregate],
    )
    agg_type_hour = fire_weather.groupBy(["type", "hour"]).agg(F.count("*").alias("count"))
    agg_type_day.coalesce(1).write.mode("overwrite").json("hdfs://localhost:8020/user/teamint/data/aggregated/type_day")
    agg_type_hour.coalesce(1).write.mode("overwrite").json(
        "hdfs://localhost:8020/user/teamint/data/aggregated/type_hour")


if __name__ == "__main__":
    main()
