from pydantic import BaseModel, Field, field_validator
from typing import Optional
import re
from datetime import datetime

class PropertyInput(BaseModel):
    """Model for raw property data from JSONL file."""
    id: str
    raw_price: str
    living_area: float
    property_type: str
    municipality: str
    scraping_date: str

    @field_validator('scraping_date')
    @classmethod
    def format_date(cls, v):
        """Convert YYYYMMDD to YYYY-MM-DD format."""
        if len(v) == 8:
            return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
        return v

class PropertyOutput(BaseModel):
    """Model for processed property data to be inserted into DuckDB."""
    id: str
    scraping_date: str
    property_type: str
    municipality: str
    price: float
    living_area: float
    price_per_square_meter: float

    @classmethod
    def from_input(cls, input_data: PropertyInput) -> Optional['PropertyOutput']:
        """Convert input data to output format."""
        try:
            # Extract numeric price from raw_price string (e.g., "530 000€/mo.")
            price_match = re.search(r'(\d+(?:\s*\d+)*)', input_data.raw_price)
            if not price_match:
                return None
                
            # Convert price string (e.g., "530 000") to float (530000.0)
            price_str = price_match.group(1).replace(" ", "")
            price = float(price_str)
            
            # Calculate price per square meter
            if input_data.living_area <= 0:
                return None
                
            price_per_sqm = round(price / input_data.living_area, 2)
            
            # Create output record
            return cls(
                id=input_data.id,
                scraping_date=input_data.scraping_date,
                property_type=input_data.property_type,
                municipality=input_data.municipality,
                price=price,
                living_area=input_data.living_area,
                price_per_square_meter=price_per_sqm
            )
        except Exception as e:
            print(f"Error processing record {input_data.id}: {str(e)}")
            return None 