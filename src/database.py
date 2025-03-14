import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from config.settings import DB_PATH, TABLE_NAME

def init_db() -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB connection and create table if it doesn't exist."""
    conn = duckdb.connect(str(DB_PATH))
    
    # Drop existing table if it exists
    conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    
    # Create table with proper PRIMARY KEY constraint
    conn.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        id VARCHAR NOT NULL PRIMARY KEY,
        scraping_date VARCHAR NOT NULL,
        property_type VARCHAR NOT NULL,
        municipality VARCHAR NOT NULL,
        price DOUBLE NOT NULL,
        living_area DOUBLE NOT NULL,
        price_per_square_meter DOUBLE NOT NULL
    )
    """)
    
    return conn

def insert_data(conn: duckdb.DuckDBPyConnection, data: List[Dict[str, Any]]) -> int:
    """Insert data into DuckDB table."""
    if not data:
        return 0
        
    # Convert to DataFrame for efficient bulk insert
    df = pd.DataFrame(data)
    
    # Ensure all required columns are present with correct types
    required_columns = [
        'id', 'scraping_date', 'property_type', 'municipality',
        'price', 'living_area', 'price_per_square_meter'
    ]
    
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns. Required: {required_columns}")
    
    # Validate data before insertion
    # Remove rows with invalid values
    df = df[
        (df['living_area'] > 0) &  # Area must be positive
        (df['price'] > 0) &        # Price must be positive
        (df['price_per_square_meter'] > 0)  # Price per sqm must be positive
    ]
    
    if df.empty:
        return 0
    
    try:
        # Insert data using a temporary table to handle duplicates
        conn.execute("""
        CREATE TEMPORARY TABLE temp_data AS SELECT * FROM df
        """)
        
        # Insert new records and update existing ones
        conn.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM temp_data
        WHERE NOT EXISTS (
            SELECT 1 FROM {TABLE_NAME}
            WHERE {TABLE_NAME}.id = temp_data.id
        )
        """)
        
        # Drop temporary table
        conn.execute("DROP TABLE temp_data")
        
        return len(df)
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        return 0

def apply_filters(conn: duckdb.DuckDBPyConnection, 
                 min_price_per_sqm: float, 
                 max_price_per_sqm: float,
                 valid_property_types: List[str],
                 min_date: str) -> int:
    """Apply filtering criteria to the data."""
    # Convert valid_property_types to a comma-separated string of quoted values
    property_types_str = ", ".join([f"'{p_type}'" for p_type in valid_property_types])
    
    # Create a temporary table with filtered data
    conn.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME}_filtered AS
    SELECT * FROM {TABLE_NAME}
    WHERE price_per_square_meter BETWEEN {min_price_per_sqm} AND {max_price_per_sqm}
    AND property_type IN ({property_types_str})
    AND scraping_date > '{min_date}'
    """)
    
    # Get count of filtered records
    result = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}_filtered").fetchone()
    filtered_count = result[0] if result else 0
    
    # Replace original table with filtered data
    conn.execute(f"DROP TABLE {TABLE_NAME}")
    conn.execute(f"""
    CREATE TABLE {TABLE_NAME} AS 
    SELECT * FROM {TABLE_NAME}_filtered
    """)
    conn.execute(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (id)")
    conn.execute(f"DROP TABLE {TABLE_NAME}_filtered")
    
    return filtered_count

def get_stats(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get statistics about the data in the table."""
    stats = {}
    
    # Total count
    result = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
    stats["total_records"] = result[0] if result else 0
    
    # Count and stats by property type
    result = conn.execute(f"""
    SELECT 
        property_type, 
        COUNT(*) as count,
        AVG(price) as avg_price,
        AVG(living_area) as avg_area,
        AVG(price_per_square_meter) as avg_price_per_sqm,
        MIN(price_per_square_meter) as min_price_per_sqm,
        MAX(price_per_square_meter) as max_price_per_sqm
    FROM {TABLE_NAME} 
    GROUP BY property_type
    ORDER BY count DESC
    """).fetchall()
    stats["property_type_stats"] = {
        r[0]: {
            "count": r[1],
            "avg_price": round(r[2], 2),
            "avg_area": round(r[3], 2),
            "avg_price_per_sqm": round(r[4], 2),
            "min_price_per_sqm": round(r[5], 2),
            "max_price_per_sqm": round(r[6], 2)
        } for r in result
    }
    
    # Municipality statistics
    result = conn.execute(f"""
    SELECT 
        municipality,
        COUNT(*) as count,
        AVG(price_per_square_meter) as avg_price_per_sqm,
        MIN(scraping_date) as earliest_date,
        MAX(scraping_date) as latest_date
    FROM {TABLE_NAME}
    GROUP BY municipality
    ORDER BY count DESC
    LIMIT 10
    """).fetchall()
    stats["top_municipalities"] = {
        r[0]: {
            "count": r[1],
            "avg_price_per_sqm": round(r[2], 2),
            "date_range": f"{r[3]} to {r[4]}"
        } for r in result
    }
    
    return stats 