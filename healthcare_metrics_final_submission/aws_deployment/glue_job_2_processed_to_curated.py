"""
Glue Job 2: Processed to Curated (Silver → Gold)
With proper logging and exception handling
"""

import sys
import logging
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, year, month, dayofmonth, quarter, weekofyear,
    dayofweek, date_format, avg, sum, min, max, count, countDistinct,
    when, row_number, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('glue_job_2_processed_to_curated')

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

SOURCE_PATH = f"s3://{args['SOURCE_BUCKET']}/processed/nursing_staffing/"
TARGET_BASE = f"s3://{args['TARGET_BUCKET']}/curated/"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def read_processed_data(path):
    """Read Parquet data from processed layer."""
    logger.info(f"Reading data from: {path}")
    try:
        df = spark.read.parquet(path)
        count = df.count()
        
        if count == 0:
            raise ValueError("No records in processed layer")
        
        logger.info(f"Read {count:,} records")
        return df
    except Exception as e:
        logger.error(f"Failed to read data: {e}")
        raise


def write_table(df, path, partition_cols=None):
    """Write DataFrame to Parquet."""
    logger.info(f"Writing to: {path}")
    try:
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)
        logger.info(f"Write complete: {df.count():,} records")
    except Exception as e:
        logger.error(f"Write failed: {e}")
        raise


# =============================================================================
# DIMENSION BUILDERS
# =============================================================================

def build_dim_date(df):
    """Build date dimension."""
    logger.info("Building dim_date...")
    try:
        dates = df.select("work_date").distinct()
        
        dim_date = dates.select(
            col("work_date").alias("full_date"),
            date_format("work_date", "yyyyMMdd").cast("int").alias("date_key"),
            year("work_date").alias("year"),
            quarter("work_date").alias("quarter"),
            month("work_date").alias("month"),
            dayofmonth("work_date").alias("day"),
            weekofyear("work_date").alias("week_of_year"),
            dayofweek("work_date").alias("day_of_week"),
            date_format("work_date", "EEEE").alias("day_name"),
            when(dayofweek("work_date").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            date_format("work_date", "yyyy-MM").alias("year_month")
        )
        
        logger.info(f"dim_date: {dim_date.count()} rows")
        return dim_date
    except Exception as e:
        logger.error(f"dim_date failed: {e}")
        raise


def build_dim_provider(df):
    """Build provider dimension."""
    logger.info("Building dim_provider...")
    try:
        dim_provider = df.select(
            "provnum", "provname", "city", "state", "county_name", "county_fips"
        ).distinct() \
         .withColumnRenamed("provnum", "provider_id") \
         .withColumnRenamed("provname", "provider_name")
        
        dim_provider = dim_provider.withColumn(
            "provider_sk", monotonically_increasing_id()
        )
        
        logger.info(f"dim_provider: {dim_provider.count()} rows")
        return dim_provider
    except Exception as e:
        logger.error(f"dim_provider failed: {e}")
        raise


def build_dim_state(df):
    """Build state dimension."""
    logger.info("Building dim_state...")
    try:
        dim_state = df.groupBy("state").agg(
            countDistinct("provnum").alias("provider_count"),
            countDistinct("county_fips").alias("county_count"),
            F.round(avg("nursing_hppd"), 2).alias("avg_hppd")
        )
        
        logger.info(f"dim_state: {dim_state.count()} rows")
        return dim_state
    except Exception as e:
        logger.error(f"dim_state failed: {e}")
        raise


# =============================================================================
# FACT TABLE BUILDER
# =============================================================================

def build_fact_daily_staffing(df):
    """Build daily staffing fact table."""
    logger.info("Building fact_daily_staffing...")
    try:
        fact = df.select(
            col("provnum").alias("provider_id"),
            date_format("work_date", "yyyyMMdd").cast("int").alias("date_key"),
            "state", "cy_qtr", "work_date",
            col("mdscensus").alias("patient_census"),
            "hrs_rn", "hrs_lpn", "hrs_cna",
            "total_nursing_hours", "nursing_hppd",
            "processed_at"
        )
        
        logger.info(f"fact_daily_staffing: {fact.count():,} rows")
        return fact
    except Exception as e:
        logger.error(f"fact_daily_staffing failed: {e}")
        raise


# =============================================================================
# AGGREGATE BUILDERS
# =============================================================================

def build_agg_state_monthly(df):
    """Build state monthly aggregate."""
    logger.info("Building agg_state_monthly...")
    try:
        agg = df.groupBy(
            "state",
            year("work_date").alias("year"),
            month("work_date").alias("month")
        ).agg(
            countDistinct("provnum").alias("provider_count"),
            F.round(avg("nursing_hppd"), 2).alias("avg_hppd"),
            F.round(avg("mdscensus"), 1).alias("avg_census"),
            count("*").alias("record_count")
        )
        
        logger.info(f"agg_state_monthly: {agg.count()} rows")
        return agg
    except Exception as e:
        logger.error(f"agg_state_monthly failed: {e}")
        raise


# =============================================================================
# MAIN
# =============================================================================

def main():
    logger.info("=" * 50)
    logger.info("STARTING GLUE JOB 2: Processed to Curated")
    logger.info("=" * 50)
    
    try:
        # Read source
        df = read_processed_data(SOURCE_PATH)
        df.cache()
        
        # Build dimensions
        dim_date = build_dim_date(df)
        dim_provider = build_dim_provider(df)
        dim_state = build_dim_state(df)
        
        # Build fact
        fact = build_fact_daily_staffing(df)
        
        # Build aggregates
        agg_state = build_agg_state_monthly(df)
        
        # Write outputs
        write_table(dim_date, f"{TARGET_BASE}dimensions/dim_date/")
        write_table(dim_provider, f"{TARGET_BASE}dimensions/dim_provider/")
        write_table(dim_state, f"{TARGET_BASE}dimensions/dim_state/")
        write_table(fact, f"{TARGET_BASE}facts/", partition_cols=["state"])
        write_table(agg_state, f"{TARGET_BASE}aggregates/agg_state_monthly/")
        
        df.unpersist()
        
        logger.info("=" * 50)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    
    finally:
        job.commit()


if __name__ == "__main__":
    main()
