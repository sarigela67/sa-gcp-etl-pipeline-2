"""NYC Taxi ETL: Raw CSVs -> Cleaned daily aggregates (Parquet)."""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, date_format, hour, 
    avg, count, when
)

def parse_args():
    parser = argparse.ArgumentParser(description="NYC Taxi ETL")
    parser.add_argument("--input_path", required=True, help="gs://.../*.csv")
    parser.add_argument("--output_path", required=True, help="gs://.../output")
    parser.add_argument("--output_mode", default="gcs", 
                       choices=["gcs", "bigquery"],
                       help="Output destination")
    parser.add_argument("--bq_table", default=None,
                       help="project.dataset.table for BigQuery")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Enterprise logging
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Starting ETL: input={args.input_path}, output={args.output_path}")
    
    spark = (SparkSession.builder
             .appName("nyc-taxi-etl")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    # EXTRACT: Read raw NYC taxi CSVs from your raw bucket
    logging.info("Reading raw data...")
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(args.input_path))
    
    logging.info(f"Raw records: {df.count()}")
    logging.info(f"Schema: {df.columns}")
    
    # TRANSFORM: Clean and aggregate
    df_clean = (df
        # Parse timestamps
        .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        
        # Calculate trip duration
        .withColumn("trip_minutes", 
                   (unix_timestamp("tpep_dropoff_datetime") - 
                    unix_timestamp("tpep_pickup_datetime")) / 60.0)
        
        # Filter bad data
        .filter(col("fare_amount") > 0)
        .filter(col("trip_minutes") > 0)
        .filter(col("trip_minutes") < 180)  # < 3 hours
        .filter(col("passenger_count").isNotNull())
        .filter(col("passenger_count") > 0)
        
        # Add features
        .withColumn("trip_date", date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
        .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .withColumn("is_weekend", 
                   when(col("trip_date").substr(1, 1).isin(["6", "7"]), 1).otherwise(0))
    )
    
    logging.info(f"Clean records: {df_clean.count()}")
    
    # AGGREGATE: Daily metrics
    agg_df = (df_clean
              .groupBy("trip_date")
              .agg(
                  avg("fare_amount").alias("avg_fare"),
                  avg("trip_minutes").alias("avg_duration_minutes"),
                  avg("trip_distance").alias("avg_distance"),
                  count("*").alias("trip_count"),
                  avg("passenger_count").alias("avg_passengers")
              )
              .orderBy(col("trip_date").desc())
    )
    
    logging.info("Writing results...")
    
    # LOAD
    if args.output_mode == "bigquery" and args.bq_table:
        (agg_df.write
         .format("bigquery")
         .option("table", args.bq_table)
         .mode("overwrite")
         .save())
        logging.info(f"Wrote to BigQuery: {args.bq_table}")
    else:
        (agg_df.write
         .mode("overwrite")
         .partitionBy("trip_date")
         .parquet(args.output_path))
        logging.info(f"Wrote Parquet to: {args.output_path}")
    
    # Show sample
    logging.info("Sample output:")
    agg_df.show(10)
    
    spark.stop()
    logging.info("ETL complete")

if __name__ == "__main__":
    main()
