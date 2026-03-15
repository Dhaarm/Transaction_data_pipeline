"""
Script: upload_to_delta.py
Purpose: Read transaction CSV and write it to Delta Lake
"""

import logging
from pyspark.sql import SparkSession

# -------------------------------------------------------
# Configure Logging
# -------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Configuration
# -------------------------------------------------------
INPUT_PATH = "/opt/data/transactions.csv"
DELTA_PATH = "/opt/data/delta"


def create_spark_session():
    """
    Create and configure Spark session with Delta Lake support
    """

    try:
        spark = SparkSession.builder \
            .appName("UploadDelta") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        logger.info("Spark session created successfully")

        return spark

    except Exception:
        logger.error("Failed to create Spark session", exc_info=True)
        raise


def read_csv_data(spark):
    """
    Read transaction CSV file into Spark DataFrame
    """

    try:
        df = spark.read.csv(
            INPUT_PATH,
            header=True,
            inferSchema=True
        )

        logger.info(f"CSV file read successfully from {INPUT_PATH}")
        logger.info(f"Total records loaded: {df.count()}")

        return df

    except FileNotFoundError:
        logger.error("CSV file not found")

    except Exception:
        logger.error("Error while reading CSV file", exc_info=True)
        raise


def write_to_delta(df):
    """
    Write DataFrame to Delta Lake table
    """

    try:
        df.write.format("delta") \
            .mode("append") \
            .save(DELTA_PATH)

        logger.info(f"Data successfully written to Delta Lake at {DELTA_PATH}")

    except Exception:
        logger.error("Error while writing to Delta Lake", exc_info=True)
        raise


def main():
    """
    Main execution workflow
    """

    spark = None

    try:
        logger.info("Starting Delta upload pipeline")

        # Create Spark session
        spark = create_spark_session()

        # Read CSV data
        df = read_csv_data(spark)

        # Write to Delta Lake
        write_to_delta(df)

        logger.info("Delta upload pipeline completed successfully")

    except Exception:
        logger.critical("Pipeline failed", exc_info=True)

    finally:
        # Ensure Spark session is stopped
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


# -------------------------------------------------------
# Entry Point
# -------------------------------------------------------
if __name__ == "__main__":
    main()