from logging import exception

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, dayofweek, lit,explode, struct, to_timestamp, count
import sys,argparse


def main(args):
    spark = SparkSession.builder \
        .appName("KLM Popular Destinations") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Load data
    try:
        bookings_df = spark.read.json(args.input_path + "/bookings/booking.json")
        airports_df = spark.read.csv(args.input_path+"/airports/airports.dat")
    except Exception as e:
        print(e)
        sys.exit(1)

    airports_df_cols = ["airport_id","name","city","country","iata","icao","latitude","longitude","altitude","timezone","dst","tz_database_time_zone","type","source"]
    airports_df = airports_df.toDF(*airports_df_cols)

    bookings_exploded = bookings_df.withColumn("passenger",
                                               explode(col("event.DataElement.travelrecord.passengersList"))) \
        .withColumn("product", explode(col("event.DataElement.travelrecord.productsList")))

    bookings_flattened = bookings_exploded.select(
        col("timestamp"),
        # Travel record fields
        col("event.DataElement.travelrecord.attributeType").alias("attributeType"),
        col("event.DataElement.travelrecord.creationDate").alias("creationDate"),
        col("event.DataElement.travelrecord.envelopNumber").alias("envelopNumber"),
        # Passenger fields
        col("passenger.age").alias("passenger_age"),
        col("passenger.category").alias("passenger_category"),
        col("passenger.crid").alias("passenger_crid"),
        col("passenger.passengerType").alias("passenger_type"),
        col("passenger.tattoo").alias("passenger_tattoo"),
        col("passenger.uci").alias("passenger_uci"),
        col("passenger.weight").alias("passenger_weight"),
        # Product fields
        col("product.aircraftType").alias("product_aircraft_type"),
        col("product.bookingClass").alias("product_booking_class"),
        col("product.bookingStatus").alias("product_booking_status"),
        col("product.flight.arrivalDate").alias("product_arrival_date"),
        col("product.flight.departureDate").alias("product_departure_date"),
        col("product.flight.originAirport").alias("product_origin_airport"),
        col("product.flight.destinationAirport").alias("product_destination_airport"),
        col("product.flight.marketingAirline").alias("product_marketing_airline"),
        col("product.flight.operatingAirline").alias("product_operating_airline"),
        col("product.transportClass").alias("product_transport_class"),
        col("product.type").alias("product_type"))

    bookings_flattened = bookings_flattened.withColumn("timestamp",
                                                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
        .withColumn("creationDate", to_timestamp(col("creationDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    filtered_bookings_df = bookings_flattened.filter(
        (col("timestamp") >= args.start_date) &
        (col("timestamp") <= args.end_date) &
        (col("product_booking_status") == "CONFIRMED") &
        (col("product_operating_airline").startswith("KL"))
    )
    joined_df = filtered_bookings_df.join(
        airports_df,
        filtered_bookings_df.product_origin_airport == airports_df.iata,
        'left'
    ).select(
        col("timestamp"),
        col("product_origin_airport"),
        col("country"),
        col("passenger_age"),
        col("passenger_category")
    )
    #
    joined_df = joined_df.withColumn(
        "day_of_week",
        dayofweek(to_date(col("timestamp"), "yyyy-MM-dd"))
    )

    # Aggregation
    result_df = joined_df.groupBy(col("country"), col("day_of_week")). \
        agg(count("*").alias("passenger_count")).orderBy(col("passenger_count").desc())

    # # Save output
    result_df.write.csv(f"{args.output_path}/report.csv", header=True)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: klm_bookings_analysis.py --input-path <input-path> --start-date <start-date> --end-date <end-date>")
        sys.exit(-1)
    parser = argparse.ArgumentParser(description="KLM Network Analysis")
    parser.add_argument("--input-path", required=True, help="Path to input booking data")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-path", default="/app/output", help="Output path for results")
    args = parser.parse_args()
    main(args)