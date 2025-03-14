import json
from pathlib import Path
from typing import List, Dict, Any, Generator
import logging

from src.models import PropertyInput, PropertyOutput

logger = logging.getLogger(__name__)

def read_jsonl(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    """Read JSONL file line by line to avoid loading entire file into memory."""
    with open(file_path, 'r') as f:
        for line in f:
            try:
                yield json.loads(line.strip())
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON line: {e}")
                continue

def process_data(input_file: Path, batch_size: int = 100) -> Generator[List[Dict[str, Any]], None, None]:
    """Process data from input file in batches."""
    batch = []
    
    for raw_record in read_jsonl(input_file):
        try:
            # Parse and validate input data
            input_data = PropertyInput(**raw_record)
            
            # Transform to output format
            output_data = PropertyOutput.from_input(input_data)
            
            if output_data:
                batch.append(output_data.dict())
                
            # Yield batch when it reaches batch_size
            if len(batch) >= batch_size:
                yield batch
                batch = []
                
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            continue
    
    # Yield remaining records
    if batch:
        yield batch 