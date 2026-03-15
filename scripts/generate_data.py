"""
Script: generate_data.py
Purpose: Generate fake customer transaction data using Faker
Author: Data Engineering Assignment
"""

import pandas as pd
import uuid
import random
import logging
from faker import Faker
from datetime import datetime

# -----------------------------------------------------------
# Configure Logging
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# Initialize Faker
# -----------------------------------------------------------
fake = Faker()

# -----------------------------------------------------------
# Configuration
# -----------------------------------------------------------
NUM_RECORDS = 1200
OUTPUT_PATH = "/opt/data/transactions.csv"

def generate_transactions(num_records: int):
    """
    Generate fake transaction records.

    Args:
        num_records (int): Number of records to generate

    Returns:
        list: List of transaction dictionaries
    """

    records = []

    try:
        for _ in range(num_records):

            # Generate unique transaction ID
            transaction_id = str(uuid.uuid4())

            # Random customer ID
            customer_id = "C" + str(random.randint(10000, 99999))

            # Random amount (including negative values intentionally)
            amount = round(random.uniform(-50, 500), 2)

            # Random timestamp within last 5 days
            timestamp = fake.date_time_between(
                start_date="-5d",
                end_date="now"
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Random merchant
            merchant = f"STORE_{random.randint(1,50)}"

            # Append record
            records.append({
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "amount": amount,
                "timestamp": timestamp,
                "merchant": merchant
            })

        logger.info(f"{num_records} records generated successfully")

    except Exception as e:
        logger.error("Error while generating transactions", exc_info=True)
        raise

    return records


def introduce_duplicates(records: list, num_duplicates: int = 20):
    """
    Introduce duplicate records intentionally for testing deduplication.

    Args:
        records (list): Original records
        num_duplicates (int): Number of duplicates to add
    """

    try:
        records.extend(records[:num_duplicates])
        logger.info(f"{num_duplicates} duplicate records added")

    except Exception:
        logger.error("Error while adding duplicates", exc_info=True)
        raise


def save_to_csv(records: list, output_path: str):
    """
    Save transaction records to CSV file.

    Args:
        records (list): List of transaction dictionaries
        output_path (str): Output file path
    """

    try:
        df = pd.DataFrame(records)

        df.to_csv(output_path, index=False)

        logger.info(f"CSV file written successfully to {output_path}")

    except PermissionError:
        logger.error("Permission denied while writing CSV file")

    except FileNotFoundError:
        logger.error("Invalid file path provided")

    except Exception:
        logger.error("Unexpected error while saving CSV", exc_info=True)
        raise


def main():
    """
    Main execution function
    """

    try:
        logger.info("Starting transaction data generation")

        # Generate records
        records = generate_transactions(NUM_RECORDS)

        # Add duplicate records
        introduce_duplicates(records)

        # Save to CSV
        save_to_csv(records, OUTPUT_PATH)

        logger.info("Data generation process completed successfully")

    except Exception as e:
        logger.critical("Script failed", exc_info=True)


# -----------------------------------------------------------
# Entry Point
# -----------------------------------------------------------
if __name__ == "__main__":
    main()