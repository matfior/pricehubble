from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

# Ensure output directory exists
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Input file path
INPUT_FILE = DATA_DIR / "scraping_data.jsonl"

# Output database path
DB_PATH = OUTPUT_DIR / "property_data.duckdb"

# Table name
TABLE_NAME = "property_data"

# Filter criteria
MIN_PRICE_PER_SQM = 500
MAX_PRICE_PER_SQM = 15000
VALID_PROPERTY_TYPES = ["apartment", "house"]
MIN_SCRAPING_DATE = "2020-03-05"  # Format: YYYY-MM-DD 