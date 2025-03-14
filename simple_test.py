#!/usr/bin/env python3
"""
Simple script to test the entire pipeline without Prefect.
"""
import json
from pathlib import Path
import duckdb
from src.models import PropertyInput, PropertyOutput
from src.database import init_db, insert_data, apply_filters
from config.settings import INPUT_FILE, DB_PATH, TABLE_NAME, MIN_PRICE_PER_SQM, MAX_PRICE_PER_SQM, VALID_PROPERTY_TYPES, MIN_SCRAPING_DATE

def print_section(title):
    """Print a section header."""
    print("\n" + "="*80)
    print(title)
    print("="*80)

def process_data():
    """Process the data without using Prefect."""
    # 1. Initialize database
    print_section("Database Initialization")
    conn = init_db()
    print("Database initialized")
    
    # 2. Read and process data
    print_section("Data Processing")
    records = []
    total_read = 0
    valid_records = 0
    
    with open(INPUT_FILE) as f:
        for line in f:
            total_read += 1
            try:
                # Parse input data
                data = json.loads(line)
                input_record = PropertyInput(**data)
                
                # Transform to output format
                output_record = PropertyOutput.from_input(input_record)
                if output_record:
                    records.append(output_record.dict())
                    valid_records += 1
                
                # Insert in batches of 100
                if len(records) >= 100:
                    insert_data(conn, records)
                    records = []
                    
            except Exception as e:
                print(f"Error processing record: {str(e)}")
    
    # Insert remaining records
    if records:
        insert_data(conn, records)
    
    print(f"Total records read: {total_read}")
    print(f"Valid records processed: {valid_records}")
    
    # 3. Apply filters
    print_section("Applying Filters")
    filtered_count = apply_filters(
        conn,
        min_price_per_sqm=MIN_PRICE_PER_SQM,
        max_price_per_sqm=MAX_PRICE_PER_SQM,
        valid_property_types=VALID_PROPERTY_TYPES,
        min_date=MIN_SCRAPING_DATE
    )
    print(f"Records after filtering: {filtered_count}")
    
    # 4. Show sample results
    print_section("Sample Results")
    result = conn.execute(f"""
    SELECT * FROM {TABLE_NAME}
    LIMIT 3
    """).fetchall()
    
    columns = ["id", "scraping_date", "property_type", "municipality", 
               "price", "living_area", "price_per_square_meter"]
    
    for record in result:
        record_dict = dict(zip(columns, record))
        print(json.dumps(record_dict, indent=2))
        
    # 5. Show statistics
    print_section("Statistics")
    stats = conn.execute(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT property_type) as unique_property_types,
        MIN(price_per_square_meter) as min_price_sqm,
        MAX(price_per_square_meter) as max_price_sqm,
        AVG(price_per_square_meter) as avg_price_sqm
    FROM {TABLE_NAME}
    """).fetchone()
    
    print(f"Total records in database: {stats[0]}")
    print(f"Unique property types: {stats[1]}")
    print(f"Price per square meter range: {stats[2]:.2f} - {stats[3]:.2f}")
    print(f"Average price per square meter: {stats[4]:.2f}")

if __name__ == "__main__":
    process_data() 