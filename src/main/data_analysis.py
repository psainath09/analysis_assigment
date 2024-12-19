import sys,argparse
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from data_ingestion import BookingDataIngestion
from data_processing import DataProcessing

class KLMNetworkAnalytics:
    def __init__(self):
        """
        Initialize Spark session and analytics components
        """
        self.spark = SparkSession.builder \
            .appName("klm_network_analytics") \
            .getOrCreate()

        self.ingestion = BookingDataIngestion(self.spark)
        self.processor = DataProcessing(self.spark)

    def run_analysis(self, input_path,output_path, start_date=None, end_date=None):
        """
        Execute complete KLM network analytics workflow

        :param input_path: Path to booking data
        :param start_date:  start date for analysis
        :param end_date:  end date for analysis
        :param output_path: Output path for analysis
        """
        # Load bookings
        bookings = self.ingestion.load_bookings(input_path)

        if bookings is None:
            print("Failed to load booking data. Exiting...")
            sys.exit(0)

        # Validate data
        if not self.ingestion.validate_data(bookings):
            print("Data validation failed. Please check input data..")
            print("Age has negative values and correcting with mean value")
            mean_age = bookings.filter(F.col("passenger_age") >= 0).agg({"passenger_age": "mean"}).collect()[0][0]
            bookings = bookings.withColumn(
                "passenger_age",
                F.when((F.col("passenger_age") < 0) | (F.col("passenger_age").isNull()), F.lit(mean_age))
                    .otherwise(F.col("passenger_age")))
        # Filter KLM flights and mentioned dates
        klm_bookings = self.ingestion.filter_klm_flights(bookings, start_date,end_date)

        airports = self.spark.read.csv(input_path + "/airports/airports.dat")
        airports_df_cols = ["airport_id", "name", "city", "country", "iata", "icao", "latitude", "longitude",
                            "altitude", "timezone", "dst", "tz_database_time_zone", "type", "source"]
        airports = airports.toDF(*airports_df_cols)

        #use broadcast so that airports can be copied to all worker nodes and avoid expensive shuffle
        joined_df = klm_bookings.join(F.broadcast(airports),klm_bookings.product_destination_airport == airports.iata,'left'
        ).select(
            F.col("timestamp"),
            F.col("product_origin_airport"),
            F.col("product_destination_airport"),
            F.col("country"),
            F.col("passenger_uci"),
            F.col("passenger_age"),
            F.col("passenger_type"),
            F.col("product_departure_date"),
            F.col("passenger_category"))

        season_bookings = joined_df.withColumn(
            "season", F.when(F.month(F.col("product_departure_date")).isin(12, 1, 2), "Winter")
            .when(F.month(F.col("product_departure_date")).isin(3, 4, 5), "Spring")
            .when(F.month(F.col("product_departure_date")).isin(6, 7, 8), "Summer")
            .otherwise("Autumn")
        ).withColumn(
            "day_of_week", F.date_format(F.col("product_departure_date"), "EEEE")
        )

        # Aggregation
        destination_analysis = self.processor.aggregate_destinations(season_bookings)
        # Show results
        print("\n--- Top Destinations ---")
        destination_analysis.show()

        # Optional: Write results to output
        destination_analysis.write.csv(f"{output_path}/report/",mode="overwrite", header=True)

    def close(self):
        """
        Close Spark session
        """
        self.spark.stop()

def main():

    # parse arguments like input_path, start_date, end_date and output_path
    parser = argparse.ArgumentParser(description="klm_analytics")
    parser.add_argument("--input_path", required=True, help="Path to input booking data")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output_path", default="/app/output", help="Output path for results")

    args = parser.parse_args()

    # Parse dates if provided
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None
    analytics = KLMNetworkAnalytics()
    try:
        analytics.run_analysis(args.input_path,args.output_path,start_date,end_date)
    finally:
        analytics.close()


if __name__ == "__main__":
    main()