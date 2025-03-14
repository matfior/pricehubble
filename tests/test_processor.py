import pytest
import json
from pathlib import Path
import tempfile

from src.models import PropertyInput, PropertyOutput

def test_property_input_date_formatting():
    """Test that the date formatting works correctly."""
    # Test with YYYYMMDD format
    input_data = PropertyInput(
        id="test123",
        raw_price="500 000€/mo.",
        living_area=100.0,
        property_type="apartment",
        municipality="Test City",
        scraping_date="20210217"
    )
    
    assert input_data.scraping_date == "2021-02-17"
    
    # Test with already formatted date
    input_data = PropertyInput(
        id="test123",
        raw_price="500 000€/mo.",
        living_area=100.0,
        property_type="apartment",
        municipality="Test City",
        scraping_date="2021-02-17"
    )
    
    assert input_data.scraping_date == "2021-02-17"

def test_property_output_from_input():
    """Test the conversion from input to output format."""
    input_data = PropertyInput(
        id="test123",
        raw_price="500 000€/mo.",
        living_area=100.0,
        property_type="apartment",
        municipality="Test City",
        scraping_date="20210217"
    )
    
    output_data = PropertyOutput.from_input(input_data)
    
    assert output_data is not None
    assert output_data.id == "test123"
    assert output_data.scraping_date == "2021-02-17"
    assert output_data.property_type == "apartment"
    assert output_data.municipality == "Test City"
    assert output_data.price == 500000.0
    assert output_data.living_area == 100.0
    assert output_data.price_per_square_meter == 5000.0

def test_property_output_price_extraction():
    """Test that price extraction works correctly for different formats."""
    # Test with space in number
    input_data = PropertyInput(
        id="test1",
        raw_price="500 000€/mo.",
        living_area=100.0,
        property_type="apartment",
        municipality="Test City",
        scraping_date="20210217"
    )
    
    output_data = PropertyOutput.from_input(input_data)
    assert output_data.price == 500000.0
    
    # Test without space
    input_data = PropertyInput(
        id="test2",
        raw_price="500000€/mo.",
        living_area=100.0,
        property_type="apartment",
        municipality="Test City",
        scraping_date="20210217"
    )
    
    output_data = PropertyOutput.from_input(input_data)
    assert output_data.price == 500000.0 