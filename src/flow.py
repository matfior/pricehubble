import logging
from pathlib import Path
from prefect import flow, task
import time
import duckdb

from config.settings import (
    INPUT_FILE, MIN_PRICE_PER_SQM, MAX_PRICE_PER_SQM, 
    VALID_PROPERTY_TYPES, MIN_SCRAPING_DATE, DB_PATH
)
from src.processor import process_data
from src.database import init_db, insert_data, apply_filters, get_stats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@task(name="Extract and Transform Data")
def extract_transform(input_file: Path, conn: duckdb.DuckDBPyConnection):
    """Extract data from JSONL file and transform it."""
    logger.info(f"Processing data from {input_file}")
    
    total_records = 0
    processed_batches = 0
    
    # Initialize database schema
    conn = init_db(conn)
    
    # Process data in batches
    for batch in process_data(input_file):
        records_inserted = insert_data(conn, batch)
        total_records += records_inserted
        processed_batches += 1
        
        logger.info(f"Processed batch {processed_batches}: {records_inserted} records inserted")
    
    logger.info(f"Total records processed: {total_records}")
    return total_records

@task(name="Apply Filters")
def filter_data(conn: duckdb.DuckDBPyConnection):
    """Apply filtering criteria to the data."""
    logger.info("Applying filters to the data")
    
    # Apply filters
    filtered_count = apply_filters(
        conn,
        min_price_per_sqm=MIN_PRICE_PER_SQM,
        max_price_per_sqm=MAX_PRICE_PER_SQM,
        valid_property_types=VALID_PROPERTY_TYPES,
        min_date=MIN_SCRAPING_DATE
    )
    
    logger.info(f"Records after filtering: {filtered_count}")
    return filtered_count

@task(name="Generate Statistics")
def generate_stats(conn: duckdb.DuckDBPyConnection):
    """Generate statistics about the processed data."""
    logger.info("Generating statistics")
    
    stats = get_stats(conn)
    
    logger.info(f"Statistics: {stats}")
    return stats

@flow(name="Property Data Pipeline")
def property_pipeline():
    """Main flow for processing property data."""
    start_time = time.time()
    logger.info("Starting property data pipeline")
    
    # Create a single database connection for the entire pipeline
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Extract and transform data
        total_records = extract_transform(INPUT_FILE, conn)
        
        # Apply filters
        filtered_count = filter_data(conn)
        
        # Generate statistics
        stats = generate_stats(conn)
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Pipeline completed in {execution_time:.2f} seconds")
        
        return {
            "total_records": total_records,
            "filtered_count": filtered_count,
            "stats": stats,
            "execution_time": f"{execution_time:.2f} seconds"
        }
    finally:
        # Always close the connection
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    property_pipeline() 