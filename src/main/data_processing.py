from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

class DataProcessing:
    def __init__(self, spark_session: SparkSession):
        """
        Initialize DataProcessor

        :param spark_session: Active Spark session
        """
        self.spark = spark_session


    def aggregate_destinations(self,
                               bookings: DataFrame) -> DataFrame:
        """
        Aggregate destination analytics
        :param bookings: Filtered booking DataFrame
        :return: Aggregated destination DataFrame
        """

        # Aggregate destinations
        return bookings.groupBy(
            F.col("product_destination_airport"),
            F.col("country"), F.col("season"), F.col("day_of_week")
        ).agg(
            F.countDistinct("passenger_uci").alias("passenger_count"),
            F.avg("passenger_age").alias("avg_age"),
            F.sum(F.when(F.col("passenger_type") == ("ADT"), 1).otherwise(0)).alias("adult_count"),
            F.sum(F.when(F.col("passenger_type") == ("CHD"), 1).otherwise(0)).alias("child_count")
        ).orderBy("passenger_count", ascending=False)
