#!/usr/bin/env python3
"""
Simple script to test data loading.
"""
import json
from pathlib import Path

# Read the first few lines of the data file
data_file = Path("data/scraping_data.jsonl")
print(f"Reading from: {data_file.absolute()}")

with open(data_file) as f:
    for i, line in enumerate(f, 1):
        if i > 5:  # Only read first 5 lines
            break
        data = json.loads(line)
        print(f"\nRecord {i}:")
        print(json.dumps(data, indent=2))

print("\nData file exists:", data_file.exists())
print("Data file size:", data_file.stat().st_size, "bytes") 