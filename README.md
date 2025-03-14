# Property Data Pipeline

A data pipeline for processing property data from a JSONL file and loading it into a DuckDB database.

## Overview

This pipeline performs the following operations:
1. Extracts property data from a JSONL file
2. Transforms the data (price extraction, date formatting, etc.)
3. Loads the data into a DuckDB database
4. Applies filtering criteria:
   - Price per square meter between 500 and 15,000
   - Property type is either "apartment" or "house"
   - Scraping date is after March 5, 2020
5. Generates statistics about the processed data

## Project Structure

```
property_pipeline/
├── config/
│   └── settings.py         # Configuration settings
├── data/
│   ├── scraping_data.jsonl # Input data
│   └── output/             # Output directory for DuckDB
├── src/
│   ├── database.py         # DuckDB operations
│   ├── flow.py             # Prefect flow definition
│   ├── models.py           # Data models
│   └── processor.py        # Data processing logic
├── tests/
│   └── test_processor.py   # Unit tests
├── Dockerfile              # Docker configuration
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt        # Python dependencies
├── run_pipeline.py         # CLI script to run the pipeline
└── README.md               # This file
```

## Prerequisites

- Python 3.8+
- Docker (optional, for containerized execution)

## Installation

### Local Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd property_pipeline
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Docker Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd property_pipeline
   ```

2. Build and start the Docker containers:
   ```bash
   docker-compose up -d
   ```

## Usage

### Running the Pipeline Locally

To run the pipeline directly without Prefect:

```bash
python run_pipeline.py --mode local
```

### Running with Prefect

1. Start the Prefect server (if not using Docker):
   ```bash
   prefect server start
   ```

2. Create and apply the deployment:
   ```bash
   python run_pipeline.py --mode prefect
   ```

3. Start a worker:
   ```bash
   prefect worker start -p default-agent-pool -t process
   ```

4. Run the deployment:
   ```bash
   prefect deployment run "Property Data Pipeline/property-pipeline-deployment"
   ```

### Using Docker

The project uses Docker Compose to run the Prefect server, worker, and PostgreSQL database:

```bash
# Start all services (Prefect, PostgreSQL, and worker)
docker-compose up -d

# Deploy the pipeline
docker-compose exec -T worker prefect deploy --all

# Run the pipeline
docker-compose exec worker prefect deployment run 'Property Data Pipeline/property-pipeline-deployment'
```

You can monitor the pipeline's progress through:
- Prefect UI: http://localhost:4200
- Command line: `docker-compose exec worker prefect flow-run ls`

To stop all services and clean up:
```bash
docker-compose down -v
```

## Configuration

### Docker Services

The project uses three main services:
- **Prefect**: Orchestration server running on http://localhost:4200
- **PostgreSQL**: Backend database for Prefect
- **Worker**: Executes the pipeline tasks

### Environment Variables

The following environment variables are configured in `docker-compose.yml`:
- `PREFECT_API_URL`: URL for the Prefect API
- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password
- `POSTGRES_DB`: PostgreSQL database name

## Output

The pipeline creates a DuckDB database file at `data/output/property_data.duckdb` with a table named `property_data` containing the processed data.

You can query this database using DuckDB's CLI or connect to it programmatically:

```python
import duckdb

# Connect to the database
conn = duckdb.connect('data/output/property_data.duckdb')

# Query the data
result = conn.execute('SELECT * FROM property_data LIMIT 10').fetchall()
print(result)
```

## Testing

Run the tests using pytest:

```bash
pytest tests/
```

## Scaling Considerations

This pipeline is designed to handle large datasets efficiently:

- Data is processed in batches to minimize memory usage
- DuckDB provides fast analytical query performance
- The pipeline can be scheduled to run periodically using Prefect

For very large datasets, consider:
- Adjusting the batch size in `src/processor.py`
- Using a distributed processing framework like Dask or Spark
- Implementing incremental loading based on scraping_date 