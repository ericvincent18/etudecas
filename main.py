import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, IntegerType

from config.elasticsearch import ElasticsearchLoader
from config.configs import config
from dev.create_data import GetSampleData

# start spark session and connect to elasticsearch
# elastic search must be started before hand
spark = (
    SparkSession.builder.appName("ElasticsearchExample")
    .config("spark.es.nodes", "localhost") # fill in with environment variables
    .config("spark.es.port", "9200")
    .getOrCreate()
)

class DataPreparation:
    def __init__(self, df):
        self.df = df

    def replace_nulls(self):
        self.df = self.df.withColumn(
            "weekly_qty", when(col("qty").isNull(), 0).otherwise(col("qty"))
        )
        self.df = self.df.withColumn(
            "product_number",
            when(col("product_number").isNull(), "Unknown").otherwise(
                col("product_number")
            ),
        )
        self.df = self.df.withColumn(
            "weekly_qty", col("weekly_qty").cast(IntegerType())
        )
        return self.df

    def transform_columns(self):
        self.df = self.df.withColumn("date", col("date").cast("date"))
        self.df = self.df.withColumn(
            "order_number", col("order_number").cast(IntegerType())
        )
        self.df = self.df.withColumn(
            "client_number", col("client_number").cast(IntegerType())
        )
        self.df = self.df.withColumn(
            "product_number", col("product_number").cast(StringType())
        )
        self.df = self.df.withColumn("SKU", col("SKU").cast(StringType()))
        self.df = self.df.withColumn(
            "weekly_qty", col("weekly_qty").cast(IntegerType())
        )
        return self.df

    def get_week_starting(self, date):
        # Find the Monday of the week that the date belongs to
        start_of_week = date - dt.timedelta(days=date.weekday())
        return start_of_week

    def aggregate_weekly_demand(self):
        self.df["date"] = pd.to_datetime(self.df["date"])
        self.df["week"] = self.df["date"].dt.isocalendar().week
        self.df["year"] = self.df["date"].dt.isocalendar().year
        self.df["week_starting"] = self.df["date"].apply(self.get_week_starting)
        df_sum = self.df.groupby(
            ["client_number", "SKU", "product_number", "week", "year", "week_starting"],
            as_index=False,
        ).agg({"weekly_qty": "sum", "date": "first"})
        return df_sum

    def get_rolling_average(self, df_sum):
        # Calculate rolling window average of 4 weeks
        df_sum["rolling_avg_qty"] = (
            df_sum.groupby(["client_number", "SKU", "product_number"])["weekly_qty"]
            .rolling(window=4, min_periods=1)
            .mean()
            .reset_index(drop=True)
        )
        return df_sum

    def get_expanding_average(self, df_sum):
        # Calculate expanding window average
        df_sum["expanding_avg_qty"] = (
            df_sum.groupby(["client_number", "SKU", "product_number"])["weekly_qty"]
            .expanding()
            .mean()
            .reset_index(drop=True)
        )
        return df_sum

    def downsample_data(self, df_sum):
        # Downsample the data 
        df_sum = df_sum.resample("W-MON", on="week_starting").last().reset_index()
        return df_sum


if __name__ == "__main__":
    sample_data = GetSampleData()
    df = sample_data.get_df

    try:
        spark_df = spark.createDataFrame(df)

        data_prep = DataPreparation(spark_df)
        transformed_df = data_prep.replace_nulls()
        transformed_df = data_prep.transform_columns()
        pandas_df = transformed_df.toPandas()
        dp = DataPreparation(pandas_df)
        df_sum = dp.aggregate_weekly_demand()
        df_sum = dp.get_rolling_average(df_sum)
        df_sum = dp.get_expanding_average(df_sum)
        df_downsampled = dp.downsample_data(df_sum)
    except Exception as e:
        print(e)

    try:
        # get Elasticsearch instance Loader
        t = ElasticsearchLoader("test_index", config.host)
        t.load_data(df_downsampled)
        # retrieve data from Elasticsearch:
        t.query_elasticsearch()
    except Exception as e:
        print(e)
