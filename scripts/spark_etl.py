"""
Script: spark_etl.py
Purpose: Read transactions from Delta Lake, clean and aggregate data,
         and load results into ScyllaDB.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from cassandra.cluster import Cluster


# ---------------------------------------------------------
# Configure Logging
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
DELTA_PATH = "/opt/data/delta"
SCYLLA_HOST = "scylladb"


# ---------------------------------------------------------
# Create Spark Session with Delta Support
# ---------------------------------------------------------
def create_spark_session():

    try:
        builder = SparkSession.builder \
            .appName("TransactionETL") \
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        logger.info("Spark session created successfully")

        return spark

    except Exception:
        logger.error("Failed to create Spark session", exc_info=True)
        raise


# ---------------------------------------------------------
# Read Delta Table
# ---------------------------------------------------------
def read_delta_table(spark):

    try:
        df = spark.read.format("delta").load(DELTA_PATH)

        logger.info("Delta table loaded successfully")
        logger.info(f"Total records read: {df.count()}")

        return df

    except Exception:
        logger.error("Error reading Delta table", exc_info=True)
        raise


# ---------------------------------------------------------
# Data Cleaning and Transformation
# ---------------------------------------------------------
def transform_data(df):

    try:
        # Remove duplicate transactions
        df = df.dropDuplicates(["transaction_id"])

        # Filter invalid transactions (negative amounts)
        df = df.filter(col("amount") > 0)

        # Extract transaction date
        df = df.withColumn(
            "transaction_date",
            to_date(col("timestamp"))
        )

        # Aggregate daily total per customer
        result = df.groupBy(
            "customer_id",
            "transaction_date"
        ).agg(
            spark_sum("amount").alias("daily_total")
        )

        logger.info("Data transformation completed")

        return result

    except Exception:
        logger.error("Error during data transformation", exc_info=True)
        raise


# ---------------------------------------------------------
# Load Data into ScyllaDB
# ---------------------------------------------------------
def load_to_scylla(result):

    try:
        cluster = Cluster([SCYLLA_HOST])
        session = cluster.connect()

        logger.info("Connected to ScyllaDB")

        # Create keyspace
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS transactions
        WITH replication = {'class':'SimpleStrategy','replication_factor':1}
        """)

        # Create table
        session.execute("""
        CREATE TABLE IF NOT EXISTS transactions.daily_customer_totals(
            customer_id TEXT,
            transaction_date DATE,
            daily_total FLOAT,
            PRIMARY KEY(customer_id, transaction_date)
        )
        """)

        # Collect results from Spark
        rows = result.collect()

        for row in rows:

            session.execute("""
            INSERT INTO transactions.daily_customer_totals
            (customer_id, transaction_date, daily_total)
            VALUES (%s,%s,%s)
            """, (row.customer_id, row.transaction_date, float(row.daily_total)))

        logger.info("Data successfully loaded into ScyllaDB")

    except Exception:
        logger.error("Error loading data into ScyllaDB", exc_info=True)
        raise


# ---------------------------------------------------------
# Demonstrate Delta Lake Versioning (Time Travel)
# ---------------------------------------------------------
def delta_time_travel_demo(spark):

    try:
        delta = DeltaTable.forPath(spark, DELTA_PATH)

        logger.info("Delta table history:")

        delta.history().show()

        # Read current version
        current = spark.read.format("delta").load(DELTA_PATH)

        # Read older version
        old = spark.read.format("delta") \
            .option("versionAsOf", 0) \
            .load(DELTA_PATH)

        logger.info(f"Current Record Count: {current.count()}")
        logger.info("*************************************************************************************************")
        logger.info(f"Old Version Record Count: {old.count()}")
        logger.info("*************************************************************************************************")

    except Exception:
        logger.error("Error demonstrating Delta Time Travel", exc_info=True)


# ---------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------
def main():

    spark = None

    try:

        logger.info("Starting Spark ETL pipeline")

        spark = create_spark_session()

        # Read Delta data
        df = read_delta_table(spark)

        # Transform data
        result = transform_data(df)

        result.show()

        # Load results to ScyllaDB
        load_to_scylla(result)

        # Demonstrate Delta versioning
        delta_time_travel_demo(spark)

        logger.info("Spark ETL pipeline completed successfully")

    except Exception:
        logger.critical("Pipeline failed", exc_info=True)

    finally:

        if spark:
            spark.stop()
            logger.info("Spark session stopped")


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()