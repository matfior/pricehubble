#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the property data pipeline')
    parser.add_argument('--mode', choices=['local', 'prefect'], default='local',
                        help='Run mode: local (direct execution) or prefect (using Prefect)')
    return parser.parse_args()

def run_local():
    """Run the pipeline locally without Prefect."""
    from src.flow import extract_transform, filter_data, generate_stats
    from config.settings import INPUT_FILE
    
    logger.info("Running pipeline in local mode")
    
    # Run pipeline steps
    extract_transform(INPUT_FILE)
    filter_data()
    generate_stats()
    
    logger.info("Pipeline completed successfully")

def run_prefect():
    """Run the pipeline using Prefect."""
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import IntervalSchedule
    from datetime import timedelta
    from src.flow import property_pipeline
    
    logger.info("Running pipeline in Prefect mode")
    
    # Create deployment
    deployment = Deployment.build_from_flow(
        flow=property_pipeline,
        name="property-pipeline-deployment",
        schedule=IntervalSchedule(interval=timedelta(days=1)),
        work_queue_name="default"
    )
    
    # Apply deployment
    deployment.apply()
    
    logger.info("Deployment created. Run 'prefect deployment run property-pipeline/property-pipeline-deployment' to execute")

if __name__ == "__main__":
    args = parse_args()
    
    if args.mode == 'local':
        run_local()
    elif args.mode == 'prefect':
        run_prefect()
    else:
        logger.error(f"Invalid mode: {args.mode}")
        sys.exit(1) 