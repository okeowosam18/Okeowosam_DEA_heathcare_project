"""
Glue Job 1: Raw to Processed (Bronze → Silver)
With proper logging and exception handling
"""

import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, when, upper, trim, lit, 
    current_timestamp, input_file_name, coalesce
)
from pyspark.sql.types import DoubleType, IntegerType

# =============================================================================
# LOGGING SETUP
# =============================================================================
# AWS Docs: https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('glue_job_1_raw_to_processed')

# =============================================================================
# INITIALIZE GLUE CONTEXT
# =============================================================================

try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'TARGET_BUCKET'])
    logger.info(f"Job arguments: {args}")
except Exception as e:
    logger.error(f"Failed to get job arguments: {e}")
    raise

try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger.info("Glue context initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Glue context: {e}")
    raise

# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_PATH = f"s3://{args['SOURCE_BUCKET']}/raw/nursing_staffing/"
TARGET_PATH = f"s3://{args['TARGET_BUCKET']}/processed/nursing_staffing/"

# =============================================================================
# MAIN ETL LOGIC
# =============================================================================

def read_source_data(path):
    """Read CSV data from source path."""
    logger.info(f"Reading data from: {path}")
    try:
        df = spark.read.option("header", "true").csv(path)
        record_count = df.count()
        
        if record_count == 0:
            raise ValueError(f"No records found in {path}")
        
        logger.info(f"Successfully read {record_count:,} records")
        return df
    
    except Exception as e:
        logger.error(f"Failed to read source data: {e}")
        raise


def validate_schema(df, required_columns):
    """Validate that required columns exist."""
    logger.info("Validating schema...")
    missing = [c for c in required_columns if c.lower() not in [col.lower() for col in df.columns]]
    
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    logger.info(f"Schema validation passed. Found {len(df.columns)} columns")
    return True


def transform_data(df):
    """Apply all transformations."""
    logger.info("Starting transformations...")
    
    try:
        # Standardize column names to lowercase
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        
        # Parse date
        df = df.withColumn("work_date", to_date(col("workdate").cast("string"), "yyyyMMdd"))
        
        # Cast numeric columns
        hour_columns = [c for c in df.columns if c.startswith('hrs_')]
        for col_name in hour_columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        df = df.withColumn("mdscensus", col("mdscensus").cast(IntegerType()))
        
        # Standardize strings
        df = df.withColumn("state", upper(trim(col("state"))))
        
        # Calculate metrics
        df = df.withColumn("total_nursing_hours",
            coalesce(col("hrs_rn"), lit(0)) +
            coalesce(col("hrs_lpn"), lit(0)) +
            coalesce(col("hrs_cna"), lit(0))
        )
        
        df = df.withColumn("nursing_hppd",
            when(col("mdscensus") > 0,
                 col("total_nursing_hours") / col("mdscensus"))
            .otherwise(None)
        )
        
        # Add metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("source_file", input_file_name())
        
        logger.info("Transformations completed successfully")
        return df
    
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise


def write_output(df, path):
    """Write data to target path as Parquet."""
    logger.info(f"Writing data to: {path}")
    
    try:
        df.write \
            .mode("overwrite") \
            .partitionBy("cy_qtr", "state") \
            .parquet(path)
        
        logger.info("Write completed successfully")
    
    except Exception as e:
        logger.error(f"Failed to write output: {e}")
        raise


def main():
    """Main ETL function."""
    logger.info("=" * 50)
    logger.info("STARTING GLUE JOB 1: Raw to Processed")
    logger.info("=" * 50)
    
    required_columns = ['provnum', 'state', 'workdate', 'mdscensus', 'hrs_rn', 'hrs_lpn', 'hrs_cna']
    
    try:
        # Extract
        df = read_source_data(SOURCE_PATH)
        
        # Validate
        validate_schema(df, required_columns)
        
        # Transform
        df_transformed = transform_data(df)
        
        # Load
        write_output(df_transformed, TARGET_PATH)
        
        logger.info("=" * 50)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise
    
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    
    finally:
        job.commit()


if __name__ == "__main__":
    main()
