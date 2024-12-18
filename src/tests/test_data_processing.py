import tempfile
import os
import json
import pytest
from pyspark.sql import SparkSession
from src.main.data_processing import DataProcessing

class TestDataProcessing:
    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder \
            .appName("data_processing_test") \
            .getOrCreate()

    def setup_method(self):
        self.processor = DataProcessing(self.spark)

        # Create sample booking data
        sample_data = [
            {
                "product_destination_airport":"CDG",
                "country":"France",
                "season":"Spring",
                "day_of_week":"Monday",
                "passenger_uci": "P1234",
                "passenger_age":"35",
                "airline": "KL",
                "origin": "AMS",
                "destination": "CDG",
                "booking_status": "Confirmed",
                "departure_date": "2023-06-15",
                "passenger_type": "Adult",
                "age": 35
            },
            {
                "product_destination_airport": "IND",
                "country": "India",
                "season": "Winter",
                "day_of_week": "Tuesday",
                "passenger_uci": "P5678",
                "passenger_age":"25",
                "airline": "KL",
                "origin": "AMS",
                "destination": "DEL",
                "booking_status": "Confirmed",
                "departure_date": "2023-07-20",
                "passenger_type": "Child",
                "age": 10
            }
        ]

        self.bookings = self.spark.createDataFrame(sample_data)

    def test_aggregate_destinations(self):
        """
        Test destination aggregation
        """
        destinations = self.processor.aggregate_destinations(self.bookings)
        assert destinations.count() > 0
