import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

class BookingDataIngestion:
    def __init__(self, spark_session: SparkSession):
        """
        :param spark_session: Active Spark session
        """
        self.spark = spark_session
        self.logger = logging.getLogger(self.__class__.__name__)
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def load_bookings(self, path: str) -> Optional[DataFrame]:
        """
        Load booking data from JSON file
        :param path: Path to JSON file or directory
        :return: Spark DataFrame or None if loading fails
        """
        try:
            # Read JSON from path
            bookings_df = self.spark.read.option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(path + "/bookings/booking.json")


            # Log total records
            record_count = bookings_df.count()
            self.logger.info(f"Loaded {record_count} booking records from {path}")

            bookings_exploded = bookings_df \
                .withColumn("passenger",F.explode(F.col("event.DataElement.travelrecord.passengersList"))) \
                .withColumn("product", F.explode(F.col("event.DataElement.travelrecord.productsList")))

            bookings_flattened = bookings_exploded.select(
                F.col("timestamp"),
                # Travel record fields
                F.col("event.DataElement.travelrecord.attributeType").alias("attributeType"),
                F.col("event.DataElement.travelrecord.creationDate").alias("creationDate"),
                F.col("event.DataElement.travelrecord.envelopNumber").alias("envelopNumber"),
                # Passenger fields
                F.col("passenger.age").alias("passenger_age"),
                F.col("passenger.category").alias("passenger_category"),
                F.col("passenger.crid").alias("passenger_crid"),
                F.col("passenger.passengerType").alias("passenger_type"),
                F.col("passenger.tattoo").alias("passenger_tattoo"),
                F.col("passenger.uci").alias("passenger_uci"),
                F.col("passenger.weight").alias("passenger_weight"),
                # Product fields
                F.col("product.aircraftType").alias("product_aircraft_type"),
                F.col("product.bookingClass").alias("product_booking_class"),
                F.col("product.bookingStatus").alias("product_booking_status"),
                F.col("product.flight.arrivalDate").alias("product_arrival_date"),
                F.col("product.flight.departureDate").alias("product_departure_date"),
                F.col("product.flight.originAirport").alias("product_origin_airport"),
                F.col("product.flight.destinationAirport").alias("product_destination_airport"),
                F.col("product.flight.marketingAirline").alias("product_marketing_airline"),
                F.col("product.flight.operatingAirline").alias("product_operating_airline"),
                F.col("product.transportClass").alias("product_transport_class"),
                F.col("product.type").alias("product_type"))

            # cast to timestamp wherever required

            bookings_flattened = bookings_flattened.withColumn("timestamp",
                                                               F.to_timestamp(F.col("timestamp"),
                                                                            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
                .withColumn("creationDate", F.to_timestamp(F.col("creationDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                .withColumn("product_arrival_date",
                            F.to_timestamp(F.col("product_arrival_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                .withColumn("product_departure_date",
                            F.to_timestamp(F.col("product_departure_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

            return bookings_flattened

        except Exception as e:
            self.logger.error(f"Error loading bookings from {path}: {str(e)}")
            return None

    def filter_klm_flights(self, bookings: DataFrame , start_date, end_date) -> DataFrame:
        """
        Filter for KLM flights from NL with confirmed bookings

        :param bookings: Input booking DataFrame
        :param start_date:  start date for analysis
        :param end_date:  end date for analysis
        :return: Filtered DataFrame
        """
        filtered_bookings_df = bookings.filter(
            (F.col("timestamp") >= start_date) &
            (F.col("timestamp") <= end_date) &
            (F.col("product_booking_status") == "CONFIRMED") &
            (F.col("product_operating_airline").startswith("KL"))
        )
        nl_airports = ["AMS", "RTM", "EIN"]  # Dutch airports
        filtered_bookings_df = filtered_bookings_df.filter(F.col("product_origin_airport").isin(nl_airports))

        return filtered_bookings_df

    def validate_data(self, bookings: DataFrame) -> bool:
        """
        Validate booking data integrity

        :param bookings: Input booking DataFrame
        :return: Boolean indicating data validity
        """

        # Check age range
        age_check = bookings.filter(
            (bookings.passenger_age < 0) | (bookings.passenger_age > 120)
        ).count() == 0

        return age_check